import os
from tempfile import NamedTemporaryFile

import pytest

import lightbus.commands.run
from lightbus import commands, BusClient
from lightbus.commands import run_command_from_args
from lightbus.config import Config
from lightbus.config.structure import RootConfig

pytestmark = pytest.mark.unit


REDIS_BUS_CONFIG = """
apis:
  default:
    event_transport:
      redis:
        url: {redis_url}
    rpc_transport:
      redis:
        url: {redis_url}
    result_transport:
      redis:
        url: {redis_url}
bus:
  schema:
    transport:
      redis:
        url: {redis_url}
"""


@pytest.yield_fixture()
async def redis_config_file(loop, redis_server_url, redis_client):
    config = REDIS_BUS_CONFIG.format(redis_url=redis_server_url)
    with NamedTemporaryFile() as f:
        f.write(config.encode("utf8"))
        f.flush()
        yield f.name
        await redis_client.execute(b"CLIENT", b"KILL", b"TYPE", b"NORMAL")


def test_commands_run_cli(mocker, redis_config_file, make_test_bus_module):
    test_bus_module = make_test_bus_module()
    m = mocker.patch.object(BusClient, "_actually_run_forever")

    args = ["--config", redis_config_file, "run", "--bus", test_bus_module]

    run_command_from_args(args)

    assert m.called


def test_commands_run_env(mocker, redis_config_file, set_env, make_test_bus_module):
    test_bus_module = make_test_bus_module()
    m = mocker.patch.object(BusClient, "_actually_run_forever")

    args = commands.parse_args(args=["run"])
    with set_env(LIGHTBUS_CONFIG=redis_config_file, LIGHTBUS_MODULE=test_bus_module):
        lightbus.commands.run.Command().handle(args, config=Config(RootConfig()))

    assert m.called


def test_commands_shell(redis_config_file, make_test_bus_module):
    test_bus_module = make_test_bus_module()
    # Prevent the shell mainloop from kicking off
    args = ["--config", redis_config_file, "shell", "--bus", test_bus_module]
    run_command_from_args(args, fake_it=True)


def test_commands_dump_schema(redis_config_file, make_test_bus_module):
    test_bus_module = make_test_bus_module()
    args = [
        "--config",
        redis_config_file,
        "dumpschema",
        "--bus",
        test_bus_module,
        "--schema",
        "/tmp/test_commands_dump_schema.json",
    ]
    try:
        os.remove("/tmp/test_commands_dump_schema.json")
    except FileNotFoundError:
        pass

    run_command_from_args(args)

    with open("/tmp/test_commands_dump_schema.json", "r") as f:
        assert len(f.read()) > 0
    os.remove("/tmp/test_commands_dump_schema.json")


def test_commands_dump_config_schema(redis_config_file, dummy_api, make_test_bus_module):
    test_bus_module = make_test_bus_module()

    args = [
        "--config",
        redis_config_file,
        "dumpconfigschema",
        "--bus",
        test_bus_module,
        "--schema",
        "/tmp/test_commands_dump_config_schema.json",
    ]
    try:
        os.remove("/tmp/test_commands_dump_config_schema.json")
    except FileNotFoundError:
        pass

    run_command_from_args(args)

    with open("/tmp/test_commands_dump_config_schema.json", "r") as f:
        assert len(f.read()) > 100
    os.remove("/tmp/test_commands_dump_config_schema.json")
