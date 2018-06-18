import os
from tempfile import NamedTemporaryFile

import pytest

import lightbus.commands.run
from lightbus import commands, BusClient
from lightbus.config import Config
from lightbus.config.structure import RootConfig

pytestmark = pytest.mark.unit


REDIS_BUS_CONFIG = """
apis:
  default:
    event_transport:
      redis:
        url: redis://{host}:{port}/0
    rpc_transport:
      redis:
        url: redis://{host}:{port}/0
    result_transport:
      redis:
        url: redis://{host}:{port}/0
bus:
  schema:
    transport:
      redis:
        url: redis://{host}:{port}/0
"""


@pytest.yield_fixture()
def redis_config_file(server):
    config = REDIS_BUS_CONFIG.format(host=server.tcp_address.host, port=server.tcp_address.port)
    with NamedTemporaryFile() as f:
        f.write(config.encode("utf8"))
        f.flush()
        yield f.name


def test_commands_run(mocker, server, redis_config_file):
    m = mocker.patch.object(BusClient, "_actually_run_forever")

    args = commands.parse_args(
        args=[
            "--config",
            redis_config_file,
            "run",
            "--bus",
            "lightbus_examples.ex01_quickstart_procedure.bus",
        ]
    )

    lightbus.commands.run.Command().handle(args, config=Config(RootConfig()))

    assert m.called


def test_commands_shell(redis_config_file):
    # Prevent the shell mainloop from kicking off
    args = commands.parse_args(
        args=[
            "--config",
            redis_config_file,
            "shell",
            "--bus",
            "lightbus_examples.ex01_quickstart_procedure.bus",
        ]
    )
    lightbus.commands.shell.Command().handle(args, config=Config(RootConfig()), fake_it=True)


def test_commands_dump_schema(redis_config_file):
    # Prevent the shell mainloop from kicking off
    args = commands.parse_args(
        args=[
            "--config",
            redis_config_file,
            "dumpschema",
            "--bus",
            "lightbus_examples.ex01_quickstart_procedure.bus",
            "--schema",
            "/tmp/test_commands_dump_schema.json",
        ]
    )
    try:
        os.remove("/tmp/test_commands_dump_schema.json")
    except FileNotFoundError:
        pass
    lightbus.commands.dump_schema.Command().handle(args, config=Config(RootConfig()))
    with open("/tmp/test_commands_dump_schema.json", "r") as f:
        assert len(f.read()) > 100
    os.remove("/tmp/test_commands_dump_schema.json")


def test_commands_dump_config_schema(redis_config_file):
    # Prevent the shell mainloop from kicking off
    args = commands.parse_args(
        args=[
            "--config",
            redis_config_file,
            "dumpschema",
            "--bus",
            "lightbus_examples.ex01_quickstart_procedure.bus",
            "--schema",
            "/tmp/test_commands_dump_config_schema.json",
        ]
    )
    try:
        os.remove("/tmp/test_commands_dump_config_schema.json")
    except FileNotFoundError:
        pass
    lightbus.commands.dump_schema.Command().handle(args, config=Config(RootConfig()))
    with open("/tmp/test_commands_dump_config_schema.json", "r") as f:
        assert len(f.read()) > 100
    os.remove("/tmp/test_commands_dump_config_schema.json")
