""" Setup pytest

This initial version of this setup has been pulled from aioredis,
as that provides fixtures for both asyncio and redis. Some
will is still required to organise the setup code below.

"""
import asyncio
from pathlib import Path
from random import randint

import pytest
import socket
import subprocess
import sys
import contextlib
import os
import ssl
import time
import logging
import tempfile
import atexit

from collections import namedtuple
from async_timeout import timeout as async_timeout

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


@pytest.fixture(scope="session")
def unused_port():
    """Gets random free port."""

    def fun():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    return fun


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
def create_redis_client(_closable, loop, request):
    """Wrapper around aioredis.create_redis."""

    async def f(*args, **kw):
        redis = await aioredis.create_redis(*args, **kw)
        _closable(redis)
        return redis

    return f


@pytest.fixture
def create_redis_pool(_closable, loop):
    """Wrapper around aioredis.create_redis_pool."""

    async def f(*args, **kw):
        redis = await aioredis.create_redis_pool(*args, **kw)
        _closable(redis)
        return redis

    return f


@pytest.fixture
async def redis_pool(create_redis_pool, server, loop):
    """Returns RedisPool instance."""
    return await create_redis_pool(server.tcp_address)


@pytest.fixture
async def redis_client(create_redis_client, server, loop):
    """Returns Redis client instance."""
    redis = await create_redis_client(server.tcp_address)
    await redis.flushall()
    return redis


@pytest.fixture
def new_redis_pool(_closable, create_redis_pool, server, loop):
    """Useful when you need multiple redis connections."""

    async def make_new(**kwargs):
        redis = await create_redis_pool(server.tcp_address, loop=loop, **kwargs)
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


@pytest.fixture(scope="session")
def server(start_redis_server):
    """Starts redis-server instance."""
    return start_redis_server("A")


@pytest.fixture(scope="session")
def redis_server_b(start_redis_server):
    """Starts redis-server instance."""
    return start_redis_server("B")


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


REDIS_SERVERS = []
REDIS_VERSIONS = {}
TEST_TIMEOUT = 30


def format_version(srv):
    return "redis_v{}".format(".".join(map(str, REDIS_VERSIONS[srv])))


@pytest.fixture(scope="session", params=REDIS_SERVERS, ids=format_version)
def redis_server_bin(request):
    """Common for start_redis_server and start_sentinel server bin path parameter.
    """
    return request.param


@pytest.fixture(scope="session")
def start_redis_server(_proc, request, unused_port, redis_server_bin):
    """Starts Redis server instance.

    Caches instances by name.
    ``name`` param -- instance alias
    ``config_lines`` -- optional list of config directives to put in config
        (if no config_lines passed -- no config will be generated,
         for backward compatibility).
    """

    version = _read_server_version(redis_server_bin)
    verbose = request.config.getoption("-v") > 3

    servers = {}

    def timeout(t):
        end = time.time() + t
        while time.time() <= end:
            yield True
        raise RuntimeError("Redis startup timeout expired")

    def maker(name, config_lines=None, *, slaveof=None):
        assert slaveof is None or isinstance(slaveof, RedisServer), slaveof
        if name in servers:
            return servers[name]

        port = unused_port()
        tcp_address = TCPAddress("localhost", port)
        if sys.platform == "win32":
            unixsocket = None
        else:
            unixsocket = "/tmp/aioredis.{}.sock".format(port)
        dumpfile = "dump-{}.rdb".format(port)
        data_dir = tempfile.gettempdir()
        dumpfile_path = os.path.join(data_dir, dumpfile)
        stdout_file = os.path.join(data_dir, "aioredis.{}.stdout".format(port))
        tmp_files = [dumpfile_path, stdout_file]
        if config_lines:
            config = os.path.join(data_dir, "aioredis.{}.conf".format(port))
            with config_writer(config) as write:
                write("daemonize no")
                write('save ""')
                write("dir ", data_dir)
                write("dbfilename", dumpfile)
                write("port", port)
                if unixsocket:
                    write("unixsocket", unixsocket)
                    tmp_files.append(unixsocket)
                write("# extra config")
                for line in config_lines:
                    write(line)
                if slaveof is not None:
                    write("slaveof {0.tcp_address.host} {0.tcp_address.port}".format(slaveof))
            args = [config]
            tmp_files.append(config)
        else:
            args = [
                "--daemonize",
                "no",
                "--save",
                '""',
                "--dir",
                data_dir,
                "--dbfilename",
                dumpfile,
                "--port",
                str(port),
            ]
            if unixsocket:
                args += ["--unixsocket", unixsocket]
            if slaveof is not None:
                args += ["--slaveof", str(slaveof.tcp_address.host), str(slaveof.tcp_address.port)]

        f = open(stdout_file, "w")
        atexit.register(f.close)
        proc = _proc(
            redis_server_bin, *args, stdout=f, stderr=subprocess.STDOUT, _clear_tmp_files=tmp_files
        )
        with open(stdout_file, "rt") as f:
            for _ in timeout(10):
                assert proc.poll() is None, ("Process terminated", proc.returncode)
                log = f.readline()
                if log and verbose:
                    print(name, ":", log, end="")
                if "The server is now ready to accept connections " in log:
                    break
            if slaveof is not None:
                for _ in timeout(10):
                    log = f.readline()
                    if log and verbose:
                        print(name, ":", log, end="")
                    if "sync: Finished with success" in log:
                        break
        info = RedisServer(name, tcp_address, unixsocket, version)
        servers.setdefault(name, info)
        return info

    return maker


@pytest.yield_fixture(scope="session")
def _proc():
    processes = []
    tmp_files = set()

    def run(*commandline, _clear_tmp_files=(), **kwargs):
        proc = subprocess.Popen(commandline, **kwargs)
        processes.append(proc)
        tmp_files.update(_clear_tmp_files)
        return proc

    try:
        yield run
    finally:
        while processes:
            proc = processes.pop(0)
            proc.terminate()
            proc.wait()
        for path in tmp_files:
            try:
                os.remove(path)
            except OSError:
                pass


def pytest_runtest_setup(item):
    # Clear out any plugins
    remove_all_plugins()

    # Clear out the API registry
    registry._apis = dict()

    # Clear out any stash command line args
    COMMAND_PARSED_ARGS.clear()


def pytest_configure(config):
    global TEST_TIMEOUT
    TEST_TIMEOUT = config.getoption("--test-timeout")
    bins = config.getoption("--redis-server")[:]
    REDIS_SERVERS[:] = bins or ["/usr/bin/redis-server"]
    REDIS_VERSIONS.update({srv: _read_server_version(srv) for srv in REDIS_SERVERS})
    assert REDIS_VERSIONS, ("Expected to detect redis versions", REDIS_SERVERS)


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
