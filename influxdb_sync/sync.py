import time
import collections
import asyncio

import aioinflux


async def get_latest_date(client:aioinflux.InfluxDBClient, measurement) -> int:
    return get_date(client, measurement, 'DESC')


def get_first_date(client:aioinflux.InfluxDBClient, measurement) -> int:
    return get_date(client, measurement, 'ASC')


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
        self.consumer_count = 2

    async def run(self) -> None:
        self.running = True
        self._stats_task = asyncio.create_task(self.write_stats())
        self._consume_task = asyncio.create_task(self.consume())
        await asyncio.gather(self.produce(), self._stats_task, self._consume_task)

    async def write_stats(self) -> None:
        while self.running:
            now = time.time()
            points_per_second = self.stats_data_send / (now - self.stats_time)
            print(f'backlog: {self.backlog.qsize()}, datapoints/s: {points_per_second}')
            self.stats_data_send = 0
            self.stats_time = now
            try:
                await asyncio.sleep(15)
            except asyncio.CancelledError:
                return

    async def produce(self) -> None:
        if not hasattr(self.src_client, 'db_info'):
            self.src_client.db_info = await DataBaseInfo.from_db(self.src_client, self.dst_db)

        producers = []
        for measurement in self.src_client.db_info.measurements.values():
            producers.append(self.produce_measurement(measurement))
        await asyncio.gather(*producers)
        
        self.running = False
    
    async def produce_measurement(self, measurement):
        print(f'reading measurement {measurement.measurement_name}')

        # overlap = 60 * 60 * 1000 * 1000 # 1 hour
        start_time = await get_first_date(self.src_client, measurement)

        while True:
            query_base = f'SELECT * FROM "{measurement.measurement_name}" WHERE time >= {start_time}'
            compare_query = f'{query_base} LIMIT 1 OFFSET {self.src_batch_size}'

            src, dst = await asyncio.gather(
                self.src_client.query(compare_query),
                self.dst_client.query(compare_query)
            )

            if src == dst:
                next_start = date_from_result(src)
                # if next start is None we are at the end of the series
                # to KISS we always sync in this case.
                if next_start is not None:
                    start_time = next_start
                    continue

            query = f'{query_base} LIMIT {self.src_batch_size}'
            now = time.time()
            entries = await self._produce_from_query(measurement, query)

            await self.backlog.put(entries)
            if len(entries) < self.src_batch_size:
                print(f'{measurement.measurement_name} done')
                return
            
            start_time = entries[-1]['time']

    async def _produce_from_query(self, measurement, query:str):
        results = await self.src_client.query(query)
        entries = []
        for result in results['results']:
            for series in result.get('series', []):
                columns = series['columns']
                for row in series['values']:
                    entry = {
                        'measurement': measurement.measurement_name,
                        'tags': {},
                        'fields': {}
                    }
                    for i, key in enumerate(columns):
                        value = row[i]
                        if key == 'time':
                            entry['time'] = value
                        elif key in measurement.tags:
                            entry['tags'][key] = value
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
                self.stats_data_send += len(batch)
            except:
                print('failed to write')
                print(batch)
                raise
            self.backlog.task_done()
        print('consumer done')
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
