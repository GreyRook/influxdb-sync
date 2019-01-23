#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `influxdb_sync` package."""

import asyncio
import random

import pytest
from aioinflux import InfluxDBClient
import aioinflux

import influxdb_sync.sync

point = {
    'time': '2009-11-10T23:00:00Z',
    'measurement': 'cpu_load_short',
    'tags': {'host': 'server01',
             'region': 'us-west'},
    'fields': {
        'value': 0.64,
        'name': 'x64',
        'cpu_count': 1
    }
}


async def ensure_influx_setup(client):
    try:
        resp = await client.query('SELECT value FROM cpu_load_short')
        return
    except aioinflux.client.InfluxDBError:
        # database does not yet exist
        pass

    await client.create_database(db='testdb')
    await client.write(point)


async def gen_test_data(client, amount, seed=42):
    r = random.Random(seed)
    t = 0
    for i in range(amount):
        batch = []
        for _ in range(1000):
            t += r.randint(10000, 1000000)
            entry = {
                'time': t,
                'measurement': 'cpu_load_short',
                'tags': {
                    'host': r.choice(['server01', 'server02', 'server03']),
                    'region': 'us-west'
                },
                'fields': {
                    'value': r.random()
                }
            }
            batch.append(entry)
        await client.write(batch)


async def compare(src_client, dst_client, query):
    src_results, dst_results = await asyncio.gather(
        src_client.query(query),
        dst_client.query(query)
    )

    src_results = src_results['results']
    dst_results = dst_results['results']

    assert len(src_results) == len(dst_results) == 1
    assert 'series' in src_results[0]
    assert 'series' in dst_results[0]
    src_series = src_results[0]['series']
    dst_series = dst_results[0]['series']
    assert len(src_series) == len(dst_series) == 1

    src_series = src_series[0]
    dst_series = dst_series[0]

    assert src_series['columns'] == dst_series['columns']

    assert len(src_series['values']) == len(dst_series['values'])
    for i, row in enumerate(src_series['values']):
        assert row == dst_series['values'][i]


@pytest.mark.asyncio
async def test_sync(influx_src, influx_dst):
    async with InfluxDBClient(port=influx_src.exposed_port, db='testdb') as src_client:
        await ensure_influx_setup(src_client)

        async with InfluxDBClient(port=influx_dst.exposed_port, db='testdb') as dst_client:
            await dst_client.create_database(db='testdb')
            syncer = influxdb_sync.sync.Synchronizer(src_client, dst_client, 'testdb', 'testdb')
            
            # test our test function
            with pytest.raises(AssertionError):
                await compare(src_client, dst_client, 'SELECT * FROM cpu_load_short')
            
            await syncer.run()

            await compare(src_client, dst_client, 'SELECT * FROM cpu_load_short')

            # generate a lot of test data
            await gen_test_data(src_client, 20)


            # test our test function
            with pytest.raises(AssertionError):
                await compare(src_client, dst_client, 'SELECT * FROM cpu_load_short')
            
            await syncer.run()

            await compare(src_client, dst_client, 'SELECT * FROM cpu_load_short')
