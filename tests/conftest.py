""" Setup pytest

This initial version of this setup has been pulled from aioredis,
as that provides fixtures for both asyncio and redis. Some
will is still required to organise the setup code below.

"""
import asyncio
from pathlib import Path
from random import randint
from urllib.parse import urlparse

import pytest
import subprocess
import sys
import contextlib
import os
import logging
import tempfile

from collections import namedtuple

import aioredis
import aioredis.sentinel
from tempfile import NamedTemporaryFile, TemporaryDirectory

import lightbus
import lightbus.creation
from lightbus import BusClient
from lightbus.api import registry
from lightbus.commands import COMMAND_PARSED_ARGS
from lightbus.path import BusPath
from lightbus.message import EventMessage
from lightbus.plugins import remove_all_plugins
from lightbus.utilities.async_tools import cancel

TCPAddress = namedtuple("TCPAddress", "host port")

RedisServer = namedtuple("RedisServer", "name tcp_address unixsocket version")

logger = logging.getLogger(__file__)

# Public fixtures


@pytest.fixture
def loop(event_loop):
    # An alias for event_loop as we have a log of tests
    # which expect the fixtured to be called loop, not event_loop
    return event_loop


@pytest.fixture
def create_redis_connection(_closable):
    """Wrapper around aioredis.create_connection."""

    @asyncio.coroutine
    def f(*args, **kw):
        conn = yield from aioredis.create_connection(*args, **kw)
        _closable(conn)
        return conn

    return f


@pytest.fixture
def redis_server_url():
    return os.environ.get("REDIS_URL", "") or "redis://localhost:6379/10"


@pytest.fixture
def redis_server_b_url():
    return os.environ.get("REDIS_URL_B", "") or "redis://localhost:6379/11"


@pytest.fixture
def redis_server_config(redis_server_url):
    parsed = urlparse(redis_server_url)
    assert parsed.scheme == "redis"
    return {
        "address": (parsed.hostname, parsed.port),
        "password": parsed.password,
        "db": int(parsed.path.strip("/") or "0"),
    }


@pytest.fixture
def redis_server_b_config(redis_server_b_url):
    return redis_server_config(redis_server_b_url)


@pytest.fixture
def create_redis_client(_closable, redis_server_config, loop, request):
    """Wrapper around aioredis.create_redis."""

    async def f(*args, **kw):
        kwargs = {}
        kwargs.update(redis_server_config)
        kwargs.update(kw)
        redis = await aioredis.create_redis(*args, **kwargs)
        _closable(redis)
        return redis

    return f


@pytest.fixture
def create_redis_pool(_closable, redis_server_config, loop):
    """Wrapper around aioredis.create_redis_pool."""

    async def f(*args, **kw):
        kwargs = {}
        kwargs.update(redis_server_config)
        kwargs.update(kw)
        redis = await aioredis.create_redis_pool(*args, **kwargs)
        _closable(redis)
        return redis

    return f


@pytest.fixture
async def redis_pool(create_redis_pool, loop):
    """Returns RedisPool instance."""
    return await create_redis_pool()


@pytest.fixture
async def redis_client(create_redis_client, loop):
    """Returns Redis client instance."""
    redis = await create_redis_client()
    await redis.flushall()
    return redis


@pytest.fixture
def new_redis_pool(_closable, create_redis_pool, loop):
    """Useful when you need multiple redis connections."""

    async def make_new(**kwargs):
        redis = await create_redis_pool(loop=loop, **kwargs)
        await redis.flushall()
        return redis

    return make_new


@pytest.yield_fixture
def _closable(loop):
    conns = []

    try:
        yield conns.append
    finally:
        waiters = []
        while conns:
            conn = conns.pop(0)
            conn.close()
            waiters.append(conn.wait_closed())
        if waiters:
            loop.run_until_complete(asyncio.gather(*waiters, loop=loop))


# Lightbus fixtures


@pytest.yield_fixture
def dummy_bus(loop):
    dummy_bus = lightbus.creation.create(
        rpc_transport=lightbus.DebugRpcTransport(),
        result_transport=lightbus.DebugResultTransport(),
        event_transport=lightbus.DebugEventTransport(),
        schema_transport=lightbus.DebugSchemaTransport(),
        plugins={},
    )
    yield dummy_bus
    dummy_bus.client.close()


@pytest.yield_fixture
async def dummy_listener(dummy_bus: BusPath, loop):
    """Start the dummy bus consuming events"""
    tasks = []

    async def listen(api_name, event_name):

        def pass_listener(*args, **kwargs):
            pass

        task = await dummy_bus.client.listen_for_event(api_name, event_name, pass_listener)
        tasks.append(task)

    try:
        yield listen
    finally:
        await cancel(*tasks)


@pytest.fixture
def get_dummy_events(mocker, dummy_bus: BusPath):
    """Get events sent on the dummy bus"""
    event_transport = dummy_bus.client.transport_registry.get_event_transport("default")
    mocker.spy(event_transport, "send_event")

    def get_events():
        events = []
        send_event_calls = event_transport.send_event.call_args_list
        for args, kwargs in send_event_calls:
            assert isinstance(
                args[0], EventMessage
            ), "Argument passed to send_event was not an EventMessage"
            events.append(args[0])
        return events

    return get_events


@pytest.fixture(name="call_rpc")
def call_rpc_fixture(bus):
    results = []

    async def call_rpc(rpc: BusPath, total, initial_delay=0.1, kwargs=None):
        await asyncio.sleep(initial_delay)
        for n in range(0, total):
            results.append(await rpc.call_async(kwargs=dict(n=n)))
        logger.warning("TEST: call_rpc() completed")
        return results

    return call_rpc


@pytest.fixture(name="consume_rpcs")
def consume_rpcs_fixture():
    # Note: If you don't cancel this manually it'll be cancelling in the loop teardown (which is
    async def consume_rpcs(bus=None, apis=None):
        await bus.client.consume_rpcs(apis=apis)
        logging.warning("TEST: consume_rpcs() completed (should not happen, should get cancelled)")

    return consume_rpcs


# Internal stuff #


def pytest_addoption(parser):
    if os.environ.get("REDIS_SERVER"):
        default_redis_server = [os.environ.get("REDIS_SERVER")]
    else:
        default_redis_server = []

    parser.addoption(
        "--redis-server",
        default=default_redis_server,
        action="append",
        help="Path to redis-server executable,"
        " defaults to value REDIS_SERVER environment variable, else `%(default)s`",
    )
    parser.addoption(
        "--test-timeout", default=30, type=int, help="The timeout for each individual test"
    )


def _read_server_version(redis_bin):
    args = [redis_bin, "--version"]
    with subprocess.Popen(args, stdout=subprocess.PIPE) as proc:
        version = proc.stdout.readline().decode("utf-8")
    for part in version.split():
        if part.startswith("v="):
            break
    else:
        raise RuntimeError("No version info can be found in {}".format(version))
    return tuple(map(int, part[2:].split(".")))


@contextlib.contextmanager
def config_writer(path):
    with open(path, "wt") as f:

        def write(*args):
            print(*args, file=f)

        yield write


TEST_TIMEOUT = 30


def pytest_runtest_setup(item):
    # Clear out any plugins
    remove_all_plugins()

    # Clear out the API registry
    registry._apis = dict()

    # Clear out any stash command line args
    COMMAND_PARSED_ARGS.clear()


@pytest.fixture
def dummy_api():
    from tests.dummy_api import DummyApi

    dummy_api = DummyApi()
    registry.add(dummy_api)
    return dummy_api


@pytest.yield_fixture
def tmp_file():
    f = NamedTemporaryFile("r+", encoding="utf8")
    yield f
    try:
        f.close()
    except IOError:
        pass


@pytest.yield_fixture
def tmp_directory():
    f = TemporaryDirectory()
    yield Path(f.name)
    try:
        f.cleanup()
    except IOError:
        pass


@pytest.fixture
def set_env():

    @contextlib.contextmanager
    def _set_env(**environ):
        old_environ = dict(os.environ)
        os.environ.update(environ)
        try:
            yield
        finally:
            os.environ.clear()
            os.environ.update(old_environ)

    return _set_env


@pytest.yield_fixture
def make_test_bus_module(mocker):
    created_modules = []

    # Prevent setup from being called, as it'll try to
    # load the schema from redis at the default location.
    # Plus this setup is needed for what this fixture is used
    # for (module loading)

    async def face_setup_async(*args, **kwargs):
        pass

    mocker.patch.object(BusClient, "setup_async", side_effect=face_setup_async)

    def inner(code: str = None):
        if code is None:
            code = "bus = lightbus.create()"

        project_name = f"test_project_{randint(1000000, 9999999)}"
        d = Path(tempfile.mkdtemp())
        os.mkdir(str(d / project_name))
        with (d / project_name / "bus.py").open("w") as bus_py:
            bus_py.write(f"import lightbus\n{code}\n")
        sys.path.insert(0, str(d))
        module_name = f"{project_name}.bus"

        # Store the module we have made so we can clean it up later
        created_modules.append((module_name, d))
        return module_name

    yield inner

    for module_name, directory in created_modules:
        if module_name in sys.modules:
            module = sys.modules[module_name]
            if hasattr(module, "bus") and isinstance(module.bus, BusPath):
                module.bus.client.close()
            sys.modules.pop(module_name)

        sys.path.remove(str(directory))
