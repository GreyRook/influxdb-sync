#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages


requirements = ['Click>=6.0', ]

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest', ]

setup(
    author="Florian Ludwig",
    author_email='f.ludwig@greyrook.com',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    description="InfluxDB sync",
    entry_points={
        'console_scripts': [
            'influxdb_sync=influxdb_sync.cli:main',
        ],
    },
    install_requires=requirements,
    license="Apache Software License 2.0",
    include_package_data=True,
    keywords='influxdb_sync',
    name='influxdb_sync',
    packages=find_packages(include=['influxdb_sync']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/FlorianLudwig/influxdb_sync',
    version='0.1.0',
    zip_safe=False,
)
