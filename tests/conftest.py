""" Setup pytest

This initial version of this setup has been pulled from aioredis,
as that provides fixtures for both asyncio and redis. Some
will is still required to organise the setup code below.

"""
import asyncio
import resource
import signal
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from queue import Queue
from random import randint
from typing import Type, Optional, Callable
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
from lightbus import (
    RedisSchemaTransport,
    DebugRpcTransport,
    DebugResultTransport,
    DebugEventTransport,
    RedisRpcTransport,
    RedisResultTransport,
    RedisEventTransport,
)
from lightbus.commands import COMMAND_PARSED_ARGS
from lightbus.config.structure import (
    RootConfig,
    ApiConfig,
    EventTransportSelector,
    RpcTransportSelector,
    ResultTransportSelector,
    SchemaTransportSelector,
    BusConfig,
    SchemaConfig,
)
from lightbus.exceptions import BusAlreadyClosed
from lightbus.path import BusPath
from lightbus.message import EventMessage
from lightbus.plugins import PluginRegistry
from lightbus.transports.redis.event import StreamUse
from lightbus.utilities.async_tools import configure_event_loop
from lightbus.utilities.internal_queue import InternalQueue
from lightbus.utilities.testing import BusQueueMockerContext

TCPAddress = namedtuple("TCPAddress", "host port")

RedisServer = namedtuple("RedisServer", "name tcp_address unixsocket version")

logger = logging.getLogger(__file__)


def pytest_sessionstart(session):
    # Set custom lightbus policy on event loop
    configure_event_loop()

    # Increase the number of allowable file descriptors to 10000
    # (needed in the reliability tests)
    min_files, max_files = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (10000, max_files))


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
async def redis_server_url():
    # We use 127.0.0.1 rather than 'localhost' as this bypasses some
    # problems we encounter with getaddrinfo when performing tests
    # which create a lot of connections (e.g. the reliability tests)
    url = os.environ.get("REDIS_URL", "") or "redis://127.0.0.1:6379/10"
    redis = await aioredis.create_redis(url)
    await redis.flushdb()
    redis.close()
    return url


@pytest.fixture
async def redis_server_b_url():
    url = os.environ.get("REDIS_URL_B", "") or "redis://127.0.0.1:6379/11"
    redis = await aioredis.create_redis(url)
    await redis.flushdb()
    redis.close()
    return url


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
def create_redis_pool(_closable, redis_server_config, loop, redis_client):
    """Wrapper around aioredis.create_redis_pool."""
    # We use the redis_client fixture to ensure that redis is flushed before each test

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
    return redis


@pytest.fixture
def new_redis_pool(_closable, create_redis_pool, loop):
    """Useful when you need multiple redis connections."""

    async def make_new(**kwargs):
        redis = await create_redis_pool(loop=loop, **kwargs)
        await redis.flushall()
        return redis

    return make_new


@pytest.fixture
def get_total_redis_connections(redis_client):
    async def _get_total_redis_connections():
        info = await redis_client.info()
        return int(info["clients"]["connected_clients"])

    return _get_total_redis_connections


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
def dummy_bus(loop, redis_server_url):
    # fmt: off
    dummy_bus = lightbus.creation.create(
        config=RootConfig(
            apis={
                'default': ApiConfig(
                    rpc_transport=RpcTransportSelector(debug=DebugRpcTransport.Config()),
                    result_transport=ResultTransportSelector(debug=DebugResultTransport.Config()),
                    event_transport=EventTransportSelector(debug=DebugEventTransport.Config()),
                )
            },
            bus=BusConfig(
                schema=SchemaConfig(
                    transport=SchemaTransportSelector(redis=RedisSchemaTransport.Config(url=redis_server_url)),
                )
            )
        ),
        plugins=[],
        _testing=True,
    )
    # fmt: on
    yield dummy_bus
    try:
        dummy_bus.client.close()
    except BusAlreadyClosed:
        pass


@pytest.yield_fixture
async def dummy_listener(dummy_bus: BusPath, loop):
    """Start the dummy bus consuming events"""
    # TODO: Remove. Barely used any more
    tasks = []

    async def listen(api_name, event_name):
        def pass_listener(*args, **kwargs):
            pass

        dummy_bus.client.listen_for_event(api_name, event_name, pass_listener, listener_name="test")

    yield listen


@pytest.fixture
def get_dummy_events(mocker, dummy_bus: BusPath):
    """Get events sent on the dummy bus"""
    event_transport = dummy_bus.client.transport_registry.get_event_transport("default")
    mocker.spy(event_transport, "send_event")

    def get_events():
        events = []
        send_event_calls = event_transport.send_event.call_args_list

        for args, kwargs in send_event_calls:
            event = args[0] if args else kwargs["event_message"]
            assert isinstance(
                event, EventMessage
            ), "Argument passed to send_event was not an EventMessage"
            events.append(event)
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
    # Clear out any stash command line args
    COMMAND_PARSED_ARGS.clear()


@pytest.fixture
def dummy_api():
    from tests.dummy_api import DummyApi

    return DummyApi()


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
def make_test_bus_module():
    """Create a python module on disk which contains a bus, and put it on the python path"""
    created_modules = []

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
                try:
                    module.bus.client.close()
                except BusAlreadyClosed:
                    # Tests may choose the close the bus of their own volition,
                    # so don't worry about it here
                    pass
            sys.modules.pop(module_name)

        sys.path.remove(str(directory))


@pytest.fixture()
def plugin_registry():
    return PluginRegistry()


def pytest_generate_tests(metafunc):
    if hasattr(metafunc.function, "also_run_in_child_thread"):
        metafunc.parametrize("thread", ["main", "child"], ids=("main_thread", "child_thread"))


def pytest_runtest_call(item):
    if hasattr(item.function, "also_run_in_child_thread"):
        if item.callspec.params.get("thread") == "child":
            exec_queue = Queue()

            def wrapper():
                try:
                    item.runtest()
                except Exception:
                    # Store trace info to allow postmortem debugging
                    type, value, tb = sys.exc_info()
                    exec_queue.put((type, value, tb))
                    tb = tb.tb_next  # Skip *this* frame
                    sys.last_type = type
                    sys.last_value = value
                    sys.last_traceback = tb
                    del type, value, tb  # Get rid of these in this frame
                    raise
                else:
                    exec_queue.put(None)

            t = threading.Thread(target=wrapper)
            t.start()
            t.join()

            exec_info = exec_queue.get()
            if exec_info:
                raise exec_info[1]


@pytest.fixture(autouse=True)
def check_for_dangling_threads():
    """Have any threads been abandoned?"""
    threads_before = set(threading.enumerate())
    yield
    threads_after = set(threading.enumerate())
    dangling_threads = threads_after - threads_before
    names = [t.name for t in dangling_threads if "ThreadPoolExecutor" not in t.name]
    assert not names, f"Some threads were left dangling: {', '.join(names)}"


def pytest_configure(config):
    config.addinivalue_line("markers", "also_run_in_child_thread: Used internally")


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
        cmd: str = None,
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
            cmd,
            "--config",
            config_path,
            "--log-level",
            "debug",
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

        try:
            p.wait(timeout=1)
        except subprocess.TimeoutExpired:
            print(f"WARNING: Shutdown timed out. Killing")
            p.kill()

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


@pytest.fixture
def queue_mocker() -> Type[BusQueueMockerContext]:
    return BusQueueMockerContext


@pytest.yield_fixture()
def error_queue():
    # TODO: Close queue
    queue = InternalQueue()
    yield queue
    assert queue.qsize() == 0, f"Errors found in error queue: {queue._queue}"


@pytest.yield_fixture()
async def stop_me_later():
    to_stop_later = []

    def _stop_me_later(*items):
        to_stop_later.extend(items)

    yield _stop_me_later

    for i in to_stop_later:
        await i.stop_server()
        await i.close_async()


class Worker:
    def __init__(self, bus_factory: Callable):
        self.bus_factory = bus_factory

    async def start(self, bus: BusPath):
        await bus.client.start_server()

    async def stop(self, bus: BusPath):
        try:
            await bus.client.stop_server()
        finally:
            # Stop server can raise any queued exceptions that had
            # not previously been raised, so make sure we
            # do the close by wrapping it in a finally
            try:
                await bus.client.close_async()
            except BusAlreadyClosed:
                pass

    def __call__(self, bus: Optional[BusPath] = None, raise_errors=True):
        bus = bus or self.bus_factory()

        @asynccontextmanager
        async def worker_context(bus):
            await self.start(bus)

            yield

            try:
                await self.stop(bus)
            except Exception as e:
                if raise_errors:
                    raise
                else:
                    logger.error(e)

        return worker_context(bus)


@pytest.yield_fixture
async def worker(new_bus):
    yield Worker(bus_factory=new_bus)


# fmt: off
@pytest.fixture
def new_bus(loop, redis_server_url):
    def _new_bus(service_name="{friendly}"):
        bus = lightbus.creation.create(
            config=RootConfig(
                apis={
                    'default': ApiConfig(
                        rpc_transport=RpcTransportSelector(
                            redis=RedisRpcTransport.Config(url=redis_server_url)
                        ),
                        result_transport=ResultTransportSelector(
                            redis=RedisResultTransport.Config(url=redis_server_url)
                        ),
                        event_transport=EventTransportSelector(redis=RedisEventTransport.Config(
                            url=redis_server_url,
                            stream_use=StreamUse.PER_EVENT,
                            service_name="test_service",
                            consumer_name="test_consumer",
                        )),
                    )
                },
                bus=BusConfig(
                    schema=SchemaConfig(
                        transport=SchemaTransportSelector(redis=RedisSchemaTransport.Config(url=redis_server_url)),
                    )
                ),
                service_name=service_name
            ),
            plugins=[],
            _testing=True,
        )
        return bus
    return _new_bus
# fmt: on
