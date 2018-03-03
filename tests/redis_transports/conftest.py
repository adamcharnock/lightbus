import asyncio
import logging

import pytest

import lightbus
from lightbus import BusNode

logger = logging.getLogger(__name__)


@pytest.fixture
def redis_rpc_transport(new_redis_pool, server, loop):
    """Get a redis transport backed by a running redis server."""
    return lightbus.RedisRpcTransport(redis_pool=new_redis_pool(maxsize=10000))


@pytest.fixture
def redis_result_transport(new_redis_pool, server, loop):
    """Get a redis transport backed by a running redis server."""
    return lightbus.RedisResultTransport(redis_pool=new_redis_pool(maxsize=10000))


@pytest.fixture
def redis_event_transport(new_redis_pool, server, loop):
    """Get a redis transport backed by a running redis server."""
    return lightbus.RedisEventTransport(redis_pool=new_redis_pool(maxsize=10000))


@pytest.fixture
def redis_schema_transport(new_redis_pool, server, loop):
    """Get a redis transport backed by a running redis server."""
    return lightbus.RedisSchemaTransport(redis_pool=new_redis_pool(maxsize=10000))


@pytest.fixture
def bus(redis_rpc_transport, redis_result_transport, redis_event_transport):
    """Get a redis transport backed by a running redis server."""
    return lightbus.create(
        rpc_transport=redis_rpc_transport,
        result_transport=redis_result_transport,
        event_transport=redis_event_transport,
    )


@pytest.fixture(name='fire_dummy_events')
def fire_dummy_events_fixture(bus):
    async def fire_dummy_events(total, initial_delay=0.1):
        await asyncio.sleep(initial_delay)
        for x in range(0, total):
            await bus.my.dummy.my_event.fire_async(field=x)
        logger.warning('TEST: fire_dummy_events() completed')
    return fire_dummy_events


@pytest.fixture(name='call_rpc')
def call_rpc_fixture(bus):
    results = []
    async def call_rpc(rpc: BusNode, total, initial_delay=0.1, kwargs=None):
        await asyncio.sleep(initial_delay)
        for n in range(0, total):
            results.append(
                await rpc.call_async(kwargs=dict(n=n))
            )
        logger.warning('TEST: call_rpc() completed')
        return results
    return call_rpc


@pytest.fixture(name='consume_rpcs')
def consume_rpcs_fixture(bus):
    # Note: You'll have to cancel this manually as it'll run forever
    async def consume_rpcs():
        await bus.bus_client.consume_rpcs()
        logging.warning('TEST: consume_rpcs() completed (should not happen, should get cancelled)')
    return consume_rpcs
