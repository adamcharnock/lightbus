import asyncio
import sys
from asyncio import Future
from concurrent.futures.thread import ThreadPoolExecutor
from concurrent.futures import wait
from functools import partial
from threading import Thread
from unittest.mock import MagicMock

import pytest

from lightbus import DebugEventTransport, RedisEventTransport, EventTransport, EventMessage
from lightbus.config import Config
from lightbus.exceptions import (
    CannotShrinkEmptyPool,
    CannotProxyPrivateMethod,
    CannotProxySynchronousMethod,
    CannotProxyProperty,
)
from lightbus.transports.pool import TransportPool

pytestmark = pytest.mark.unit


@pytest.fixture()
async def dummy_pool():
    pool = TransportPool(
        transport_class=DebugEventTransport,
        transport_config=DebugEventTransport.Config(),
        config=Config.default(),
    )
    yield pool
    await pool.close()


@pytest.fixture()
async def redis_pool(redis_server_url):
    pool = TransportPool(
        transport_class=RedisEventTransport,
        transport_config=RedisEventTransport.Config(url=redis_server_url),
        config=Config.default(),
    )
    yield pool
    await pool.close()


@pytest.fixture()
async def attr_test_pool(redis_server_url):
    """Used for testing attribute access only"""

    class AttrTestEventTransport(EventTransport):
        class_prop = True

        async def async_method(self):
            return True

        def sync_method(self):
            return True

        async def _async_private_method(self):
            return True

    pool = TransportPool(
        transport_class=AttrTestEventTransport,
        transport_config=AttrTestEventTransport.Config(),
        config=Config.default(),
    )
    yield pool
    await pool.close()


@pytest.fixture()
def run_in_many_threads():
    def run_in_many_threads_(async_fn, max_workers=50, executions=200, *args, **kwargs):
        with ThreadPoolExecutor(max_workers=max_workers) as e:
            futures = []
            for _ in range(0, executions):
                futures.append(e.submit(partial(asyncio.run, async_fn(*args, **kwargs))))

            done, _ = wait(futures)

        for future in done:
            if future.exception():
                raise future.exception()

        return done

    return run_in_many_threads_


def test_equal(dummy_pool):
    """Test the __eq__ method"""
    assert dummy_pool == TransportPool(
        transport_class=DebugEventTransport,
        transport_config=DebugEventTransport.Config(),
        config=Config.default(),
    )


def test_not_equal(redis_pool, redis_server_url):
    """Test the __eq__ method"""
    assert redis_pool != TransportPool(
        transport_class=RedisEventTransport,
        transport_config=RedisEventTransport.Config(url=redis_server_url, service_name="123"),
        config=Config.default(),
    )


def test_hash_equal(dummy_pool):
    """Test the __hash__ method"""
    assert hash(dummy_pool) == hash(
        TransportPool(
            transport_class=DebugEventTransport,
            transport_config=DebugEventTransport.Config(),
            config=Config.default(),
        )
    )


def test_hash_not_equal(redis_pool, redis_server_url):
    """Test the __hash__ method"""
    assert hash(redis_pool) != hash(
        TransportPool(
            transport_class=RedisEventTransport,
            transport_config=RedisEventTransport.Config(url=redis_server_url, service_name="123"),
            config=Config.default(),
        )
    )


@pytest.mark.asyncio
async def test_grow(dummy_pool: TransportPool):
    assert dummy_pool.free == 0
    assert dummy_pool.in_use == 0

    await dummy_pool.grow()
    assert dummy_pool.free == 1
    assert dummy_pool.in_use == 0

    await dummy_pool.grow()
    assert dummy_pool.free == 2
    assert dummy_pool.in_use == 0


def test_grow_threaded(redis_pool: TransportPool, run_in_many_threads):
    run_in_many_threads(redis_pool.grow, executions=200)

    assert redis_pool.free == 200
    assert redis_pool.in_use == 0


@pytest.mark.asyncio
async def test_shrink(dummy_pool: TransportPool):
    await dummy_pool.grow()
    await dummy_pool.grow()

    await dummy_pool.shrink()
    await dummy_pool.shrink()
    assert dummy_pool.free == 0


@pytest.mark.asyncio
async def test_shrink_threaded(redis_pool: TransportPool, run_in_many_threads):
    for _ in range(0, 200):
        await redis_pool.grow()

    assert redis_pool.free == 200

    run_in_many_threads(redis_pool.shrink, executions=200)

    assert redis_pool.free == 0
    assert redis_pool.in_use == 0


@pytest.mark.asyncio
async def test_shrink_when_empty(dummy_pool: TransportPool):
    with pytest.raises(CannotShrinkEmptyPool):
        await dummy_pool.shrink()


@pytest.mark.asyncio
async def test_checkout_checkin(dummy_pool: TransportPool):
    transport = await dummy_pool.checkout()
    assert isinstance(transport, DebugEventTransport)
    assert dummy_pool.free == 0
    assert dummy_pool.in_use == 1

    await dummy_pool.checkin(transport)
    assert dummy_pool.free == 1
    assert dummy_pool.in_use == 0


@pytest.mark.asyncio
async def test_checking_to_closed_transport(mocker, redis_pool: TransportPool):
    transport = await redis_pool.checkout()
    mocker.spy(transport, "close")

    # Close the pool
    await redis_pool.close()

    # Should work even though pool is closed
    await transport.send_event(EventMessage(api_name="api", event_name="event"), options={})

    # Check the transport into the closed pool
    await redis_pool.checkin(transport)

    # The transport has been closed by the pool
    assert transport.close.called


@pytest.mark.asyncio
async def test_checkout_checkin_threaded(
    mocker, redis_pool: TransportPool, run_in_many_threads, get_total_redis_connections
):
    """Check in/out many connections concurrently (using threads)

    Note that this will not grow the pool. See the doc string for TransportPool
    """

    mocker.spy(redis_pool, "grow")
    mocker.spy(redis_pool, "_create_transport")
    mocker.spy(redis_pool, "_close_transport")

    async def _check_in_out():
        transport = await redis_pool.checkout()
        # Ensure we do something here in order to slow down the execution
        # time, thereby ensuring our pool starts to fill up. We also need to
        # use the connection to ensure the connection is lazy loaded
        await transport.send_event(EventMessage(api_name="api", event_name="event"), options={})
        await asyncio.sleep(0.02)
        await redis_pool.checkin(transport)

    run_in_many_threads(_check_in_out, executions=500, max_workers=20)

    # We're running in a thread, so we never grow the pool. Instead
    # we just return one-off connections which will be closed
    # when they get checked back in
    assert not redis_pool.grow.called
    assert redis_pool._create_transport.call_count == 500
    assert redis_pool._close_transport.call_count == 500
    assert await get_total_redis_connections() == 1


@pytest.mark.asyncio
async def test_checkout_checkin_asyncio(
    mocker, redis_pool: TransportPool, get_total_redis_connections
):
    """Check in/out many connections concurrently (using asyncio tasks)

    Unlike using threads, this should grow the pool
    """
    # mocker.spy in pytest-mock runs afoul of this change in 3.8.1
    #    https://bugs.python.org/issue38857
    # We therefore use mocker.spy for python 3.7, or the new AsyncMock in 3.8
    # See: https://github.com/pytest-dev/pytest-mock/issues/178
    if sys.version_info >= (3, 8):
        from unittest.mock import AsyncMock

        redis_pool.grow = AsyncMock(wraps=redis_pool.grow)
        redis_pool._create_transport = AsyncMock(wraps=redis_pool._create_transport)
        redis_pool._close_transport = AsyncMock(wraps=redis_pool._close_transport)
    else:
        mocker.spy(redis_pool, "grow")
        mocker.spy(redis_pool, "_create_transport")
        mocker.spy(redis_pool, "_close_transport")

    async def _check_in_out():
        transport = await redis_pool.checkout()
        # Ensure we do something here in order to slow down the execution
        # time, thereby ensuring our pool starts to fill up. We also need to
        # use the connection to ensure the connection is lazy loaded
        await transport.send_event(EventMessage(api_name="api", event_name="event"), options={})
        await asyncio.sleep(0.02)
        await redis_pool.checkin(transport)

    async def _check_in_out_loop():
        for _ in range(0, 500 // 20):
            await _check_in_out()

    tasks = [asyncio.create_task(_check_in_out_loop()) for _ in range(0, 20)]
    await asyncio.wait(tasks)
    await redis_pool.close()

    # Stop the test being flakey
    await asyncio.sleep(0.2)

    assert redis_pool.grow.call_count == 20
    assert redis_pool._create_transport.call_count == 20
    assert redis_pool._close_transport.call_count == 20
    assert await get_total_redis_connections() == 1


@pytest.mark.asyncio
async def test_context(dummy_pool: TransportPool):
    async with dummy_pool as transport:
        assert isinstance(transport, DebugEventTransport)

    assert dummy_pool.free == 1
    assert dummy_pool.in_use == 0


@pytest.mark.asyncio
async def test_close(dummy_pool: TransportPool):
    await dummy_pool.grow()
    transport = dummy_pool.pool[0]

    # Fancy footwork to mock the close coroutine
    # (because it is async, not sync)
    f = Future()
    f.set_result(None)
    transport.close = MagicMock(return_value=f)

    await dummy_pool.close()
    assert transport.close.called

    assert dummy_pool.free == 0
    assert dummy_pool.in_use == 0


@pytest.mark.asyncio
async def test_proxy_attr_async(attr_test_pool: TransportPool):
    # Passes method call through to underlying transport
    result = await attr_test_pool.async_method()
    assert result

    # Transport was created, used, and is now freed up
    assert attr_test_pool.free == 1
    assert attr_test_pool.in_use == 0


@pytest.mark.asyncio
async def test_proxy_sync(attr_test_pool: TransportPool):
    # Will raise errors as pass-through is only available for async public methods
    with pytest.raises(CannotProxySynchronousMethod):
        await attr_test_pool.sync_method()

    assert attr_test_pool.free == 0
    assert attr_test_pool.in_use == 0


@pytest.mark.asyncio
async def test_proxy_async_private(attr_test_pool: TransportPool):
    # Will raise errors as pass-through is only available for async public methods
    with pytest.raises(CannotProxyPrivateMethod):
        await attr_test_pool._async_private_method()

    assert attr_test_pool.free == 0
    assert attr_test_pool.in_use == 0


@pytest.mark.asyncio
async def test_proxy_property(attr_test_pool: TransportPool):
    # Will raise errors as pass-through is only available for async public methods
    with pytest.raises(CannotProxyProperty):
        _ = attr_test_pool.class_prop

    assert attr_test_pool.free == 0
    assert attr_test_pool.in_use == 0


@pytest.mark.asyncio
async def test_attr_proxy_not_found(attr_test_pool: TransportPool):
    with pytest.raises(AttributeError):
        _ = attr_test_pool.foo

    assert attr_test_pool.free == 0
    assert attr_test_pool.in_use == 0


@pytest.mark.asyncio
async def test_close_with_transports_from_child_thread(redis_pool: TransportPool):
    """Test that the pool can be closed when it contains transports created within a child thread"""
    flag = False

    async def thread():
        nonlocal flag
        async with redis_pool as transport:
            # Do something to ensure a connection gets made
            async for _ in transport.history("foo", "bar"):
                pass
            flag = True

    fn = partial(asyncio.run, thread())
    t = Thread(target=fn)
    t.start()
    t.join()

    # Ensure we actually went and connected to redis
    assert flag

    await redis_pool.close()
    assert redis_pool.in_use == 0
    assert redis_pool.free == 0
