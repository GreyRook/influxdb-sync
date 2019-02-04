import time
import collections
import asyncio

import aioinflux


async def get_date(client:aioinflux.InfluxDBClient, measurement, order:str) -> int:
    query = f'SELECT * FROM "{measurement.measurement_name}" ORDER BY time {order} LIMIT 1'
    try:
        results = await client.query(query)
        return date_from_result(results)
    except aioinflux.client.InfluxDBError:
        return None


def date_from_result(results):
    results = results.get('results')
    series = results[0].get('series')
    if series:
        values = series[0]['values']
        return values[0][0]
    return None


class Synchronizer:
    def __init__(self, src_client:aioinflux.InfluxDBClient, dst_client:aioinflux.InfluxDBClient,
                 src_db:str=None, dst_db:str=None, max_queue_size=50) -> None:
        self.src_client = src_client
        self.dst_client = dst_client
        self.backlog = asyncio.Queue(maxsize=max_queue_size)
        self.src_db = src_db
        self.dst_db = dst_db
        self.src_batch_size = 5000
        self.dst_batch_size = 5000
        self.stats_data_send = 0
        self.stats_time = time.time()
        self.consumer_count = 4
        self.producer_count = 10
        self.reset_stats()

    def reset_stats(self):
        self.skipped_points = 0
        self.modified_points = 0

    async def run(self) -> None:
        self.running = True
        self._stats_task = asyncio.create_task(self.write_stats())
        self._consume_task = asyncio.create_task(self.consume())
        await asyncio.gather(self.produce(), self._stats_task, self._consume_task)

    async def write_stats(self) -> None:
        while self.running:
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                return

            now = time.time()
            points_per_second = self.stats_data_send / (now - self.stats_time) / 1000
            skipped_points = self.skipped_points / 1000000
            modified_points = self.modified_points / 1000000
            print(f'backlog: {self.backlog.qsize()}, '
                  f'skipped {skipped_points:.2f}M, '
                  f'writes {modified_points:.2f}M, '
                  f'datapoints/s: {points_per_second:.2f}k')
            self.stats_data_send = 0
            self.stats_time = now

    async def produce(self) -> None:
        self._producer_sem = asyncio.Semaphore(self.producer_count)

        if not hasattr(self.src_client, 'db_info'):
            self.src_client.db_info = await DataBaseInfo.from_db(self.src_client, self.dst_db)

        for measurement in self.src_client.db_info.measurements.values():
            await self.series_producer(measurement)
        
        # ensure all tasks are done
        for _ in range(self.producer_count):
            await self._producer_sem.acquire()
        
        self.running = False


    async def series_producer(self, measurement):
        query = f'SHOW SERIES CARDINALITY FROM {measurement.measurement_name}'
        result = await self.src_client.query(query)
        cardinality = result['results'][0]['series'][0]['values'][0][0]

        print(f'measurement {measurement.measurement_name} with {cardinality} cardinality')

        for soffset in range(cardinality):
            worker = self.produce_series(measurement, soffset)
            await self._producer_sem.acquire()
            asyncio.ensure_future(worker)

    async def produce_series(self, measurement, soffset):
        try:
            await self._produce_worker(measurement, soffset)
        finally:
            self._producer_sem.release()

    async def _produce_worker(self, measurement, soffset):
        select_clause = f'SELECT * FROM "{measurement.measurement_name}"'
        slimit_clause = f'SLIMIT 1 SOFFSET {soffset}'
        start_time = 0

        max_offset_multiplier = 16
        check_offset = self.src_batch_size * max_offset_multiplier

        while True:
            query_base = f'{select_clause} WHERE time >= {start_time} GROUP BY * '
            compare_query = f'{query_base} LIMIT 1 OFFSET {check_offset} {slimit_clause}'

            src, dst = await asyncio.gather(
                self.src_client.query(compare_query),
                self.dst_client.query(compare_query)
            )

            if src == dst:
                next_start = date_from_result(src)
                if next_start is None:
                    if max_offset_multiplier != 1:
                        check_offset //= 2
                        max_offset_multiplier /= 2
                        continue
                else:
                    start_time = next_start
                    self.skipped_points += check_offset
                    self.stats_data_send += check_offset
                    if check_offset < self.src_batch_size * max_offset_multiplier:
                        check_offset *= 2
                    continue

            check_offset = self.src_batch_size

            query = f'{query_base} LIMIT {self.src_batch_size} {slimit_clause}'
            entries = await self._produce_from_query(measurement, query)

            await self.backlog.put(entries)
            if len(entries) < self.src_batch_size:
                return 

            start_time = entries[-1]['time']


    async def _produce_from_query(self, measurement, query:str):
        results = await self.src_client.query(query)
        entries = []
        for result in results['results']:
            for series in result.get('series', []):
                columns = series['columns']
                tags = series.get('tags', {})
                for row in series['values']:
                    entry = {
                        'measurement': measurement.measurement_name,
                        'tags': tags,
                        'fields': {}
                    }
                    for i, key in enumerate(columns):
                        value = row[i]
                        if key == 'time':
                            entry['time'] = value
                        # elif key in measurement.tags:
                        #     entry['tags'][key] = value
                        elif value is not None:
                            entry['fields'][key] = measurement.values[key](value)

                    entries.append(entry)

        return entries

    async def consume(self):
        consumers = []
        for _ in range(self.consumer_count):
            consumers.append(self._consume())

        try:
            await asyncio.gather(*consumers)
        except asyncio.CancelledError:
            pass

    def _check_done(self):
        if self.backlog.empty():
            self._stats_task.cancel()
            self._consume_task.cancel()

    async def _consume(self):
        while self.running or not self.backlog.empty():
            batch = await self.backlog.get()

            try:
                await asyncio.shield(self.dst_client.write(batch))
                point_count = len(batch)
                self.stats_data_send += point_count
                self.modified_points += point_count
            except asyncio.CancelledError:
                return
            except Exception as e:
                print('failed to write')
                print(batch)
                raise e
            self.backlog.task_done()
        self._check_done()


class MeasurementInfo:
    type_map = {
        'float': float,
        'string': str,
        'integer': int,
        'boolean': bool
    }

    def __init__(self, db_name:str, measurement_name:str, tags:set, values:dict):
        self.db_name = db_name
        self.measurement_name = measurement_name
        self.tags = tags
        self.values = values

    @classmethod
    async def from_db(self, client, db_name, measurement_name):
        # XXX prepared statements / escaping needed
        query = f'SHOW TAG KEYS ON "{db_name}" FROM "{measurement_name}"'
        keys_result = await client.query(query)

        query = f'SHOW FIELD KEYS ON "{db_name}" FROM "{measurement_name}"'
        values_result = await client.query(query)

        key_names = set()
        for result in keys_result['results']:
            for series in result['series']:
                assert series['columns'] == ['tagKey']
                for value in series['values']:
                    key_names.add(value[0])


        value_types = {}
        for result in values_result['results']:
            for series in result['series']:
                assert series['columns'] == ['fieldKey', 'fieldType']
                for field_key, field_type in series['values']:
                    value_types[field_key] = self.type_map[field_type]

        return MeasurementInfo(db_name, measurement_name, key_names, value_types)


class DataBaseInfo:
    def __init__(self):
        self.measurements = {}

    @classmethod
    async def from_db(cls, client:aioinflux.InfluxDBClient, db_name:str):
        # XXX prepared statements / escaping needed
        query = f'SHOW measurements ON {db_name}'
        measurements = await client.query(query)

        measurement_names = []
        for result in measurements['results']:
            for series in result['series']:
                assert series['columns'] == ['name']
                for value in series['values']:
                    measurement_names.append(value[0])

        measurement_infos = []
        for measurement_name in measurement_names:
            future = MeasurementInfo.from_db(client, db_name, measurement_name)
            measurement_infos.append(future)

        instance = DataBaseInfo()
        measurements = await asyncio.gather(*measurement_infos)
        instance.measurements = {m.measurement_name: m for m in measurements}
        return instance


class ServerInfo:
    pass
