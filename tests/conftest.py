""" Setup pytest

This initial version of this setup has been pulled from aioredis,
as that provides fixtures for both asyncio and redis. Some
will is still required to organise the setup code below.

"""
import asyncio
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

import lightbus
from lightbus.api import registry
from lightbus.bus import BusNode
from lightbus.message import EventMessage
from lightbus.plugins import remove_all_plugins

TCPAddress = namedtuple('TCPAddress', 'host port')

RedisServer = namedtuple('RedisServer', 'name tcp_address unixsocket version')

SentinelServer = namedtuple('SentinelServer',
                            'name tcp_address unixsocket version masters')

# Public fixtures


@pytest.yield_fixture
def loop():
    """Creates new event loop."""
    loop = asyncio.new_event_loop()
    loop.set_debug(enabled=True)
    asyncio.set_event_loop(None)

    try:
        yield loop
    finally:
        if hasattr(loop, 'is_closed'):
            closed = loop.is_closed()
        else:
            closed = loop._closed   # XXX
        if not closed:
            loop.call_soon(loop.stop)
            loop.run_forever()
            loop.close()


@pytest.fixture(scope='session')
def unused_port():
    """Gets random free port."""
    def fun():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            return s.getsockname()[1]
    return fun


@pytest.fixture
def create_redis_connection(_closable, loop):
    """Wrapper around aioredis.create_connection."""

    @asyncio.coroutine
    def f(*args, **kw):
        kw.setdefault('loop', loop)
        conn = yield from aioredis.create_connection(*args, **kw)
        _closable(conn)
        return conn
    return f


@pytest.fixture()
def create_redis_client(_closable, loop, request):
    """Wrapper around aioredis.create_redis."""
    @asyncio.coroutine
    def f(*args, **kw):
        kw.setdefault('loop', loop)
        redis = yield from aioredis.create_redis(*args, **kw)
        _closable(redis)
        return redis
    return f


@pytest.fixture
def create_redis_pool(_closable, loop):
    """Wrapper around aioredis.create_redis_pool."""

    @asyncio.coroutine
    def f(*args, **kw):
        kw.setdefault('loop', loop)
        redis = yield from aioredis.create_redis_pool(*args, **kw)
        _closable(redis)
        return redis
    return f


@pytest.fixture
def redis_pool(create_redis_pool, server, loop):
    """Returns RedisPool instance."""
    pool = loop.run_until_complete(
        create_redis_pool(server.tcp_address, loop=loop))
    return pool


@pytest.fixture
def redis_client(create_redis_client, server, loop):
    """Returns Redis client instance."""
    redis = loop.run_until_complete(
        create_redis_client(server.tcp_address, loop=loop))
    loop.run_until_complete(redis.flushall())
    return redis


@pytest.fixture
def new_redis_client(create_redis_client, server, loop):
    """Useful when you need multiple redis connections."""
    def make_new():
        redis = loop.run_until_complete(
            create_redis_client(server.tcp_address, loop=loop))
        loop.run_until_complete(redis.flushall())
        return redis
    return make_new


@pytest.fixture
def new_redis_pool(_closable, create_redis_pool, server, loop):
    """Useful when you need multiple redis connections."""
    def make_new(**kwargs):
        redis = loop.run_until_complete(
            create_redis_pool(server.tcp_address, loop=loop, **kwargs)
        )
        loop.run_until_complete(redis.flushall())
        # TODO: Tests that error often claim redis tasks are still running.
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


@pytest.fixture(scope='session')
def server(start_redis_server):
    """Starts redis-server instance."""
    return start_redis_server('A')


@pytest.fixture(scope='session')
def redis_server_b(start_redis_server):
    """Starts redis-server instance."""
    return start_redis_server('B')

# Lightbus fixtures

@pytest.fixture
def dummy_bus():
    return lightbus.create(
        rpc_transport=lightbus.DebugRpcTransport(),
        result_transport=lightbus.DebugResultTransport(),
        event_transport=lightbus.DebugEventTransport(),
    )


@pytest.yield_fixture
def event_consumer(dummy_bus: BusNode, loop, mocker):
    task = None

    async def dummy_coroutine(*args, **kwargs):
        pass

    m = mocker.patch.object(dummy_bus.bus_client.event_transport.__class__, 'send_event', return_value=dummy_coroutine())

    def get_events():
        events = []
        for args, kwargs in m.call_args_list:
            assert isinstance(args[0], EventMessage), 'Argument passed to send_event was not an EventMessage'
            events.append(args[0])
        return events

    try:
        task = asyncio.ensure_future(
            dummy_bus.bus_client.consume_events(),
            loop=loop
        )
        yield get_events
    finally:
        if task is not None:
            task.cancel()
            loop.run_until_complete(task)


# Internal stuff #


def pytest_addoption(parser):
    parser.addoption('--redis-server', default=[],
                     action="append",
                     help="Path to redis-server executable,"
                          " defaults to `%(default)s`")
    parser.addoption('--test-timeout', default=30,
                     type=int,
                     help="The timeout for each individual test")


def _read_server_version(redis_bin):
    args = [redis_bin, '--version']
    with subprocess.Popen(args, stdout=subprocess.PIPE) as proc:
        version = proc.stdout.readline().decode('utf-8')
    for part in version.split():
        if part.startswith('v='):
            break
    else:
        raise RuntimeError(
            "No version info can be found in {}".format(version))
    return tuple(map(int, part[2:].split('.')))


@contextlib.contextmanager
def config_writer(path):
    with open(path, 'wt') as f:
        def write(*args):
            print(*args, file=f)
        yield write


REDIS_SERVERS = []
REDIS_VERSIONS = {}
TEST_TIMEOUT = 30


def format_version(srv):
    return 'redis_v{}'.format('.'.join(map(str, REDIS_VERSIONS[srv])))


@pytest.fixture(scope='session', params=REDIS_SERVERS, ids=format_version)
def redis_server_bin(request):
    """Common for start_redis_server and start_sentinel server bin path parameter.
    """
    return request.param


@pytest.fixture(scope='session')
def start_redis_server(_proc, request, unused_port, redis_server_bin):
    """Starts Redis server instance.

    Caches instances by name.
    ``name`` param -- instance alias
    ``config_lines`` -- optional list of config directives to put in config
        (if no config_lines passed -- no config will be generated,
         for backward compatibility).
    """

    version = _read_server_version(redis_server_bin)
    verbose = request.config.getoption('-v') > 3

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
        tcp_address = TCPAddress('localhost', port)
        if sys.platform == 'win32':
            unixsocket = None
        else:
            unixsocket = '/tmp/aioredis.{}.sock'.format(port)
        dumpfile = 'dump-{}.rdb'.format(port)
        data_dir = tempfile.gettempdir()
        dumpfile_path = os.path.join(data_dir, dumpfile)
        stdout_file = os.path.join(data_dir, 'aioredis.{}.stdout'.format(port))
        tmp_files = [dumpfile_path, stdout_file]
        if config_lines:
            config = os.path.join(data_dir, 'aioredis.{}.conf'.format(port))
            with config_writer(config) as write:
                write('daemonize no')
                write('save ""')
                write('dir ', data_dir)
                write('dbfilename', dumpfile)
                write('port', port)
                if unixsocket:
                    write('unixsocket', unixsocket)
                    tmp_files.append(unixsocket)
                write('# extra config')
                for line in config_lines:
                    write(line)
                if slaveof is not None:
                    write("slaveof {0.tcp_address.host} {0.tcp_address.port}"
                          .format(slaveof))
            args = [config]
            tmp_files.append(config)
        else:
            args = ['--daemonize', 'no',
                    '--save', '""',
                    '--dir', data_dir,
                    '--dbfilename', dumpfile,
                    '--port', str(port),
                    ]
            if unixsocket:
                args += [
                    '--unixsocket', unixsocket,
                    ]
            if slaveof is not None:
                args += [
                    '--slaveof',
                    str(slaveof.tcp_address.host),
                    str(slaveof.tcp_address.port),
                    ]

        f = open(stdout_file, 'w')
        atexit.register(f.close)
        proc = _proc(redis_server_bin, *args,
                     stdout=f,
                     stderr=subprocess.STDOUT,
                     _clear_tmp_files=tmp_files)
        with open(stdout_file, 'rt') as f:
            for _ in timeout(10):
                assert proc.poll() is None, (
                    "Process terminated", proc.returncode)
                log = f.readline()
                if log and verbose:
                    print(name, ":", log, end='')
                if 'The server is now ready to accept connections ' in log:
                    break
            if slaveof is not None:
                for _ in timeout(10):
                    log = f.readline()
                    if log and verbose:
                        print(name, ":", log, end='')
                    if 'sync: Finished with success' in log:
                        break
        info = RedisServer(name, tcp_address, unixsocket, version)
        servers.setdefault(name, info)
        return info

    return maker


@pytest.fixture(scope='session')
def start_sentinel(_proc, request, unused_port, redis_server_bin):
    """Starts Redis Sentinel instances."""
    version = _read_server_version(redis_server_bin)
    verbose = request.config.getoption('-v') > 3

    sentinels = {}

    def timeout(t):
        end = time.time() + t
        while time.time() <= end:
            yield True
        raise RuntimeError("Redis startup timeout expired")

    def maker(name, *masters, quorum=1, noslaves=False):
        key = (name,) + masters
        if key in sentinels:
            return sentinels[key]
        port = unused_port()
        tcp_address = TCPAddress('localhost', port)
        data_dir = tempfile.gettempdir()
        config = os.path.join(
            data_dir, 'aioredis-sentinel.{}.conf'.format(port))
        stdout_file = os.path.join(
            data_dir, 'aioredis-sentinel.{}.stdout'.format(port))
        tmp_files = [config, stdout_file]
        if sys.platform == 'win32':
            unixsocket = None
        else:
            unixsocket = os.path.join(
                data_dir, 'aioredis-sentinel.{}.sock'.format(port))
            tmp_files.append(unixsocket)

        with config_writer(config) as write:
            write('daemonize no')
            write('save ""')
            write('port', port)
            if unixsocket:
                write('unixsocket', unixsocket)
            write('loglevel debug')
            for master in masters:
                write('sentinel monitor', master.name,
                      '127.0.0.1', master.tcp_address.port, quorum)
                write('sentinel down-after-milliseconds', master.name, '3000')
                write('sentinel failover-timeout', master.name, '3000')

        f = open(stdout_file, 'w')
        atexit.register(f.close)
        proc = _proc(redis_server_bin,
                     config,
                     '--sentinel',
                     stdout=f,
                     stderr=subprocess.STDOUT,
                     _clear_tmp_files=tmp_files)
        # XXX: wait sentinel see all masters and slaves;
        all_masters = {m.name for m in masters}
        if noslaves:
            all_slaves = {}
        else:
            all_slaves = {m.name for m in masters}
        with open(stdout_file, 'rt') as f:
            for _ in timeout(30):
                assert proc.poll() is None, (
                    "Process terminated", proc.returncode)
                log = f.readline()
                if log and verbose:
                    print(name, ":", log, end='')
                for m in masters:
                    if '# +monitor master {}'.format(m.name) in log:
                        all_masters.discard(m.name)
                    if '* +slave slave' in log and \
                            '@ {}'.format(m.name) in log:
                        all_slaves.discard(m.name)
                if not all_masters and not all_slaves:
                    break
            else:
                raise RuntimeError("Could not start Sentinel")

        masters = {m.name: m for m in masters}
        info = SentinelServer(name, tcp_address, unixsocket, version, masters)
        sentinels.setdefault(key, info)
        return info
    return maker


@pytest.fixture(scope='session')
def ssl_proxy(_proc, request, unused_port):
    by_port = {}

    cafile = os.path.abspath(request.config.getoption('--ssl-cafile'))
    certfile = os.path.abspath(request.config.getoption('--ssl-cert'))
    dhfile = os.path.abspath(request.config.getoption('--ssl-dhparam'))
    assert os.path.exists(cafile), \
        "Missing SSL CA file, run `make certificate` to generate new one"
    assert os.path.exists(certfile), \
        "Missing SSL CERT file, run `make certificate` to generate new one"
    assert os.path.exists(dhfile), \
        "Missing SSL DH params, run `make certificate` to generate new one"

    if hasattr(ssl, 'create_default_context'):
        ssl_ctx = ssl.create_default_context(cafile=cafile)
    else:
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        ssl_ctx.load_verify_locations(cafile=cafile)
    if hasattr(ssl_ctx, 'check_hostname'):
        # available since python 3.4
        ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    ssl_ctx.load_dh_params(dhfile)

    def sockat(unsecure_port):
        if unsecure_port in by_port:
            return by_port[unsecure_port]

        secure_port = unused_port()
        _proc('/usr/bin/socat',
              'openssl-listen:{port},'
              'dhparam={param},'
              'cert={cert},verify=0,fork'
              .format(port=secure_port, param=dhfile, cert=certfile),
              'tcp-connect:localhost:{}'
              .format(unsecure_port)
              )
        time.sleep(1)   # XXX
        by_port[unsecure_port] = secure_port, ssl_ctx
        return secure_port, ssl_ctx

    return sockat


@pytest.yield_fixture(scope='session')
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


@pytest.mark.tryfirst
def pytest_pycollect_makeitem(collector, name, obj):
    if collector.funcnamefilter(name):
        if not callable(obj):
            return
        item = pytest.Function(name, parent=collector)
        if 'run_loop' in item.keywords:
            # TODO: re-wrap with asyncio.coroutine if not native coroutine
            return list(collector._genfunctions(name, obj))


@pytest.mark.tryfirst
def pytest_pyfunc_call(pyfuncitem):
    """
    Run asyncio marked test functions in an event loop instead of a normal
    function call.
    """
    if 'run_loop' in pyfuncitem.keywords:
        marker = pyfuncitem.keywords['run_loop']
        funcargs = pyfuncitem.funcargs
        loop = funcargs['loop']
        testargs = {arg: funcargs[arg]
                    for arg in pyfuncitem._fixtureinfo.argnames}
        
        loop.run_until_complete(
            _wait_coro(pyfuncitem.obj, testargs,
                       timeout=marker.kwargs.get('timeout', TEST_TIMEOUT),
                       loop=loop))
        return True


@asyncio.coroutine
def _wait_coro(corofunc, kwargs, timeout, loop):
    with async_timeout(timeout, loop=loop):
        return (yield from corofunc(**kwargs))


def pytest_runtest_setup(item):
    # Clear out any plugins
    remove_all_plugins()

    # Clear out the API registry
    registry._apis = dict()

    if 'run_loop' in item.keywords and 'loop' not in item.fixturenames:
        # inject an event loop fixture for all async tests
        item.fixturenames.append('loop')


def pytest_collection_modifyitems(session, config, items):
    for item in items:
        if 'redis_version' in item.keywords:
            marker = item.keywords['redis_version']
            try:
                version = REDIS_VERSIONS[item.callspec.getparam('redis_server_bin')]
            except (KeyError, ValueError, AttributeError):
                # TODO: throw noisy warning
                continue
            if version < marker.kwargs['version']:
                item.add_marker(pytest.mark.skip(
                    reason=marker.kwargs['reason']))


def pytest_configure(config):
    global TEST_TIMEOUT
    TEST_TIMEOUT = config.getoption('--test-timeout')
    bins = config.getoption('--redis-server')[:]
    REDIS_SERVERS[:] = bins or ['/usr/bin/redis-server']
    REDIS_VERSIONS.update({srv: _read_server_version(srv)
                           for srv in REDIS_SERVERS})
    assert REDIS_VERSIONS, ("Expected to detect redis versions", REDIS_SERVERS)


def logs(logger, level=None):
    """Catches logs for given logger and level.

    See unittest.TestCase.assertLogs for details.
    """
    return _AssertLogsContext(logger, level)


_LoggingWatcher = namedtuple("_LoggingWatcher", ["records", "output"])


class _CapturingHandler(logging.Handler):
    """
    A logging handler capturing all (raw and formatted) logging output.
    """

    def __init__(self):
        logging.Handler.__init__(self)
        self.watcher = _LoggingWatcher([], [])

    def flush(self):
        pass

    def emit(self, record):
        self.watcher.records.append(record)
        msg = self.format(record)
        self.watcher.output.append(msg)


class _AssertLogsContext:
    """Standard unittest's _AssertLogsContext context manager
    adopted to raise pytest failure.
    """
    LOGGING_FORMAT = "%(levelname)s:%(name)s:%(message)s"

    def __init__(self, logger_name, level):
        self.logger_name = logger_name
        if level:
            self.level = level
        else:
            self.level = logging.INFO
        self.msg = None

    def __enter__(self):
        if isinstance(self.logger_name, logging.Logger):
            logger = self.logger = self.logger_name
        else:
            logger = self.logger = logging.getLogger(self.logger_name)
        formatter = logging.Formatter(self.LOGGING_FORMAT)
        handler = _CapturingHandler()
        handler.setFormatter(formatter)
        self.watcher = handler.watcher
        self.old_handlers = logger.handlers[:]
        self.old_level = logger.level
        self.old_propagate = logger.propagate
        logger.handlers = [handler]
        logger.setLevel(self.level)
        logger.propagate = False
        return handler.watcher

    def __exit__(self, exc_type, exc_value, tb):
        self.logger.handlers = self.old_handlers
        self.logger.propagate = self.old_propagate
        self.logger.setLevel(self.old_level)
        if exc_type is not None:
            # let unexpected exceptions pass through
            return False
        if len(self.watcher.records) == 0:
            pytest.fail(
                "no logs of level {} or higher triggered on {}"
                .format(logging.getLevelName(self.level), self.logger.name))


def redis_version(*version, reason):
    assert 1 < len(version) <= 3, version
    assert all(isinstance(v, int) for v in version), version
    return pytest.mark.redis_version(version=version, reason=reason)


def assert_almost_equal(first, second, places=None, msg=None, delta=None):
    assert not (places is None and delta is None), \
        "Both places and delta are not set, please set one"
    if delta is not None:
        assert abs(first - second) <= delta
    else:
        assert round(abs(first - second), places) == 0


def pytest_namespace():
    return {
        'assert_almost_equal': assert_almost_equal,
        'redis_version': redis_version,
        'logs': logs,
    }


@pytest.fixture
def dummy_api():
    from tests.dummy_api import DummyApi
    dummy_api = DummyApi()
    registry.add(dummy_api)
    return dummy_api
