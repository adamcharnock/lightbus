from asyncio import Future
from unittest.mock import MagicMock

import pytest

from lightbus import DebugEventTransport, RedisEventTransport, EventTransport
from lightbus.config import Config
from lightbus.exceptions import (
    CannotShrinkEmptyPool,
    CannotProxyPrivateMethod,
    CannotProxySynchronousMethod,
    CannotProxyProperty,
)
from lightbus.transports.pool import TransportPool

pytestmark = pytest.mark.unit


@pytest.yield_fixture()
def dummy_pool():
    return TransportPool(
        transport_class=DebugEventTransport,
        transport_config=DebugEventTransport.Config(),
        config=Config.default(),
    )


@pytest.yield_fixture()
def redis_pool(redis_server_url):
    return TransportPool(
        transport_class=RedisEventTransport,
        transport_config=RedisEventTransport.Config(url=redis_server_url),
        config=Config.default(),
    )


@pytest.yield_fixture()
def attr_test_pool(redis_server_url):
    """Used for testing attribute access only"""

    class AttrTestEventTransport(EventTransport):
        class_prop = True

        async def async_method(self):
            return True

        def sync_method(self):
            return True

        async def _async_private_method(self):
            return True

    return TransportPool(
        transport_class=AttrTestEventTransport,
        transport_config=AttrTestEventTransport.Config(),
        config=Config.default(),
    )


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


@pytest.mark.asyncio
async def test_shrink(dummy_pool: TransportPool):
    await dummy_pool.grow()
    await dummy_pool.grow()

    await dummy_pool.shrink()
    await dummy_pool.shrink()
    assert dummy_pool.free == 0


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
async def test_context(dummy_pool: TransportPool):
    async with dummy_pool as transport:
        assert isinstance(transport, DebugEventTransport)

    assert dummy_pool.free == 1
    assert dummy_pool.in_use == 0


@pytest.mark.asyncio
async def test_open(dummy_pool: TransportPool):
    await dummy_pool.open()
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
