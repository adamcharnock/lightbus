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
    install_requires=["aioredis>=1.2.0,<2", "jsonschema>=2.6.0,<3", "pyyaml", "python-dateutil"],
    extras_require={':python_version=="3.6"': ["dataclasses==0.6"]},
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
