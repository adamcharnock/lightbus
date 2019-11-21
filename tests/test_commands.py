import json
import logging
import os
import signal
import time
from subprocess import Popen

import pytest

from tests.conftest import BUS_MODULE

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.integration


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

    run_lightbus_command("dumpschema", "--out", "/tmp/test_commands_dump_schema.json")
    time.sleep(1)

    with open("/tmp/test_commands_dump_schema.json", "r") as f:
        assert len(f.read()) > 0
    os.remove("/tmp/test_commands_dump_schema.json")


def test_commands_dump_config_schema(run_lightbus_command):
    try:
        os.remove("/tmp/test_commands_dump_config_schema.json")
    except FileNotFoundError:
        pass

    run_lightbus_command("dumpconfigschema", "--out", "/tmp/test_commands_dump_config_schema.json")
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
