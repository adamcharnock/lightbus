import json
import logging
import os
import signal
import subprocess
import sys
import time
from subprocess import Popen
from tempfile import NamedTemporaryFile

import pytest

logger = logging.getLogger(__name__)

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

DEBUG_BUS_CONFIG = """
apis:
  default:
    event_transport:
      debug: {}
    rpc_transport:
      debug: {}
    result_transport:
      debug: {}
bus:
  schema:
    transport:
      debug: {}
"""

BUS_MODULE = """
import lightbus

bus = lightbus.create()

class DummyApi(lightbus.Api):
    my_event = lightbus.Event()

    class Meta:
        name = "my.dummy"


bus.client.register_api(DummyApi())
"""


@pytest.yield_fixture()
async def redis_config_file(loop, redis_server_url, redis_client):
    config = REDIS_BUS_CONFIG.format(redis_url=redis_server_url)
    with NamedTemporaryFile() as f:
        f.write(config.encode("utf8"))
        f.flush()
        yield f.name
        await redis_client.execute(b"CLIENT", b"KILL", b"TYPE", b"NORMAL")


@pytest.yield_fixture()
def debug_config_file():
    with NamedTemporaryFile() as f:
        f.write(DEBUG_BUS_CONFIG.encode("utf8"))
        f.flush()
        yield f.name


@pytest.yield_fixture()
def run_lightbus_command(make_test_bus_module, redis_config_file):
    processes = []

    def inner(
        cmd: str,
        *args: str,
        env: dict = None,
        bus_module_code: str = None,
        config_path: str = None,
        full_args: list = None,
    ):
        env = env or {}

        # Create a bus module and tell lightbus where to find it
        env.setdefault("LIGHTBUS_MODULE", make_test_bus_module(code=bus_module_code))
        # Set the python path so we can load the bus module we've just create
        env.setdefault("PYTHONPATH", ":".join(sys.path))

        # Set the PATH so the 'lightbus' command can be found
        env.setdefault("PATH", os.environ.get("PATH", ""))

        config_path = config_path or redis_config_file
        full_args = full_args or [
            "lightbus",
            "--config",
            config_path,
            "--log-level",
            "debug",
            cmd,
            *args,
        ]

        logger.debug(f"Running: {' '.join(full_args)}. Environment: {env}")

        p = subprocess.Popen(full_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        processes.append((cmd, full_args, env, p))

        # Let it startup
        time.sleep(1)

        return p

    yield inner

    # Cleanup
    for cmd, full_args, env, p in processes:
        try:
            os.kill(p.pid, signal.SIGINT)
        except ProcessLookupError:
            # Process already gone
            pass

        p.wait(timeout=1)

        print(f"Cleaning up command 'lightbus {cmd}'")
        print(f"     Command: {' '.join(full_args)}")
        print(f"     Environment:")
        for k, v in env.items():
            print(f"         {k.ljust(20)}: {v}")

        print(f"---- 'lightbus {cmd}' stdout ----", flush=True)
        print(p.stdout.read().decode("utf8"), flush=True)

        print(f"---- 'lightbus {cmd}' stderr ----", flush=True)
        print(p.stderr.read().decode("utf8"), flush=True)
        assert p.returncode == 0, f"Child process running 'lightbus {cmd}' exited abnormally"


def test_commands_run_cli(run_lightbus_command, make_test_bus_module):
    run_lightbus_command("run", "--bus", make_test_bus_module(), env={"LIGHTBUS_MODULE": ""})


def test_commands_run_env(run_lightbus_command, redis_config_file, make_test_bus_module):
    run_lightbus_command(
        "run", env={"LIGHTBUS_MODULE": make_test_bus_module(), "LIGHTBUS_CONFIG": redis_config_file}
    )


def test_commands_shell(run_lightbus_command):
    run_lightbus_command("shell")


def test_commands_dump_schema(run_lightbus_command):
    try:
        os.remove("/tmp/test_commands_dump_schema.json")
    except FileNotFoundError:
        pass

    run_lightbus_command("dumpschema", "--schema", "/tmp/test_commands_dump_schema.json")
    time.sleep(1)

    with open("/tmp/test_commands_dump_schema.json", "r") as f:
        assert len(f.read()) > 0
    os.remove("/tmp/test_commands_dump_schema.json")


def test_commands_dump_config_schema(run_lightbus_command):
    try:
        os.remove("/tmp/test_commands_dump_config_schema.json")
    except FileNotFoundError:
        pass

    run_lightbus_command(
        "dumpconfigschema", "--schema", "/tmp/test_commands_dump_config_schema.json"
    )
    time.sleep(1)

    with open("/tmp/test_commands_dump_config_schema.json", "r") as f:
        assert len(f.read()) > 0
    os.remove("/tmp/test_commands_dump_config_schema.json")


def test_commands_inspect_simple(run_lightbus_command, debug_config_file):
    process: Popen = run_lightbus_command(
        "inspect", "--api", "my.dummy", config_path=debug_config_file, bus_module_code=BUS_MODULE
    )
    time.sleep(1)

    lines = process.stdout.readlines()
    assert lines
    for line in lines:
        event = json.loads(line)
        assert event["event_name"]


def test_commands_inspect_follow(run_lightbus_command, debug_config_file):
    process: Popen = run_lightbus_command(
        "inspect", "--follow", config_path=debug_config_file, bus_module_code=BUS_MODULE
    )
    # Let it run a few seconds and it should keep popping out messages
    time.sleep(3)

    # Should still be running
    assert process.poll() is None, "Inspect process unexpectedly died"
    os.kill(process.pid, signal.SIGINT)

    lines = process.stdout.readlines()
    assert len(lines) > 2
    for line in lines:
        event = json.loads(line)
        assert event["event_name"]
