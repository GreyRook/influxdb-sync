import time
import collections
import asyncio

import aioinflux


class Synchronizer:
    def __init__(self, src_client, dst_client, src_db=None, dst_db=None):
        self.src_client = src_client
        self.dst_client = dst_client
        self.backlog = asyncio.Queue(maxsize=50)
        self.src_db = src_db
        self.dst_db = dst_db
        self.src_batch_size = 5000
        self.dst_batch_size = 5000
        self.stats_data_send = 0
        self.stats_time = time.time()

    async def run(self):
        self.running = True
        await asyncio.gather(self.produce(), self.consume(), self.write_stats())

    async def write_stats(self) -> None:
        while self.running:
            now = time.time()
            points_per_second = self.stats_data_send / (now - self.stats_time)
            print(f'backlog: {self.backlog.qsize()}, datapoints/s: {points_per_second}')
            self.stats_data_send = 0
            self.stats_time = now
            await asyncio.sleep(15)

    async def produce(self) -> None:
        if not hasattr(self.src_client, 'db_info'):
            self.src_client.db_info = await DataBaseInfo.from_db(self.src_client, self.dst_db)

        producers = []
        for measurement in self.src_client.db_info.measurements.values():
            producers.append(self.produce_measurement(measurement))
        await asyncio.gather(*producers)
        
        self.running = False
    
    async def produce_measurement(self, measurement) -> None:
        print(f'reading measurement {measurement.measurement_name}')

        overlap = 60 * 60 * 1000 * 1000 # 1 hour
        query = f'SELECT * FROM "{measurement.measurement_name}" ORDER BY time DESC LIMIT 1'
        try:
            result = await self.dst_client.query(query)
        except aioinflux.client.InfluxDBError:
            results = {}

        result = result.get('results')
        series = result[0].get('series')
        if series:
            values = series[0]['values']
            start_time = values[0][0]
            print(f'{measurement.measurement_name}: found existing data', start_time) 
            start_time -= overlap
        else:
            print(f'{measurement.measurement_name}: no existing data')
            start_time = 0
        
        while True:
            query = f'SELECT * FROM "{measurement.measurement_name}" WHERE time >= {start_time} LIMIT {self.src_batch_size}'
            now = time.time()
            entries = await self._produce_from_query(measurement, query)
            print(f'recv took {time.time() - now}')
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
        while self.running or not self.backlog.empty():
            batch = await self.backlog.get()
           
            try: 
              await self.dst_client.write(batch)
              self.stats_data_send += len(batch)
            except:
              print('failed to write')
              print(batch)
              raise
            self.backlog.task_done()
        print('consume done')


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
    async def from_db(self, client, db_name):
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
