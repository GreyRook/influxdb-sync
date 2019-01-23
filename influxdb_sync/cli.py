# -*- coding: utf-8 -*-

"""Console script for influxdb_sync."""
import sys
import click
import functools
import asyncio

from aioinflux import InfluxDBClient

from . import sync


def async_cli(f):
    def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(f(*args, **kwargs))
    return functools.update_wrapper(wrapper, f)


@click.command()
@click.option('--src', default='localhost')
@click.option('--dst', default='localhost')
@click.option('--src-port', default=8086)
@click.option('--dst-port', default=8086)
@click.option('--src-username', default=None)
@click.option('--dst-username', default=None)
@click.option('--src-password', default=None)
@click.option('--dst-password', default=None)
@click.option('--db')
@async_cli
async def main(src, dst, src_port, dst_port, src_username, dst_username, src_password, dst_password, db, args=None):
    print(src, dst, src_port, dst_port)
    async with InfluxDBClient(host=src, port=src_port, username=src_username, password=src_password, db=db) as src_client:
        async with InfluxDBClient(host=dst, port=dst_port, username=dst_username, password=dst_password, db=db) as dst_client:
            await dst_client.create_database(db=db)
            syncer = sync.Synchronizer(src_client, dst_client, db, db)
            await syncer.run()

    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
