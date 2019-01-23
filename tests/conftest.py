import dockerdb.influxdb_pytest


influx_src = dockerdb.influxdb_pytest.fixture(scope='module', versions=['1.5.4'], exposed_port=8200)
influx_dst = dockerdb.influxdb_pytest.fixture(scope='module', versions=['1.5.4'], exposed_port=8201)