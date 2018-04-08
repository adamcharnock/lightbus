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
    return lightbus.RedisEventTransport(redis_pool=new_redis_pool(maxsize=10000), consumer_group_name='test_cg')


@pytest.fixture
def redis_schema_transport(new_redis_pool, server, loop):
    """Get a redis transport backed by a running redis server."""
    logger.debug('Loop: {}'.format(id(loop)))
    return lightbus.RedisSchemaTransport(redis_pool=new_redis_pool(maxsize=10000))


@pytest.fixture
def bus(loop, redis_rpc_transport, redis_result_transport, redis_event_transport, redis_schema_transport):
    """Get a redis transport backed by a running redis server."""
    return lightbus.create(
        rpc_transport=redis_rpc_transport,
        result_transport=redis_result_transport,
        event_transport=redis_event_transport,
        schema_transport=redis_schema_transport,
        loop=loop,
    )


@pytest.fixture(name='fire_dummy_events')
def fire_dummy_events_fixture(bus):
    async def fire_dummy_events(total, initial_delay=0.1):
        await asyncio.sleep(initial_delay)
        for x in range(0, total):
            await bus.my.dummy.my_event.fire_async(field=x)
        logger.warning('TEST: fire_dummy_events() completed')
    return fire_dummy_events
