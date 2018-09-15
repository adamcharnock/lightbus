#!/usr/bin/env python

from os.path import exists
from setuptools import setup, find_packages

setup(
    name="lightbus",
    version=open("VERSION").read().strip(),
    author="Adam Charnock",
    author_email="adam@adamcharnock.com",
    packages=find_packages(exclude=("lightbus_examples", "experiments", "tests")),
    scripts=[],
    url="http://lightbus.org",
    license="MIT",
    description="Filling the gap between monolithic and microservice",
    long_description=open("README.rst").read() if exists("README.rst") else "",
    install_requires=[
        "aioredis",  # TODO: Pin version once streams support is merged
        "jsonschema>=2.6.0,<3",
        "pyyaml",
        "python-dateutil",
    ],
    extras_require={
        "development": [
            "pre-commit",
            "black",
            "hiredis==0.2.0",
            "flake8==3.4.1",
            "coverage==4.4.1",
            "pytest==3.2.3",
            "pytest-cov==2.5.1",
            "pytest-xdist==1.20.1",
            "pytest-mock==1.6.3",
            "pytest-repeat==0.4.1",
            "async-timeout==1.4.0",
            "pytest-catchlog==1.2.2",
            "pytest-asyncio==0.9.0",
            "coveralls==1.2.0",
            # Note for future: Should not be installed when testing against py >= 3.7
            "dataclasses==0.6",
            "psycopg2-binary==2.7.4",
            "aiopg==0.14.0",
            # Docs
            "mkdocs==0.17.4",
            "mkdocs-material==2.9.0",
            "bpython",
        ]
    },
    include_package_data=True,
    entry_points={
        "console_scripts": ["lightbus = lightbus.commands:lightbus_entry_point"],
        "lightbus_plugins": [
            "internal_state = lightbus.plugins.state:StatePlugin",
            "internal_metrics = lightbus.plugins.metrics:MetricsPlugin",
        ],
        "lightbus_event_transports": [
            "redis = lightbus:RedisEventTransport",
            "debug = lightbus:DebugEventTransport",
            "direct = lightbus:DirectEventTransport",
            "transactional = lightbus:TransactionalEventTransport",
        ],
        "lightbus_rpc_transports": [
            "redis = lightbus:RedisRpcTransport",
            "debug = lightbus:DebugRpcTransport",
            "direct = lightbus:DirectRpcTransport",
        ],
        "lightbus_result_transports": [
            "redis = lightbus:RedisResultTransport",
            "debug = lightbus:DebugResultTransport",
            "direct = lightbus:DirectResultTransport",
        ],
        "lightbus_schema_transports": ["redis = lightbus:RedisSchemaTransport"],
    },
)
