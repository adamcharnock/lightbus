import asyncio
import logging

import pytest
from aioredis import create_redis_pool

import lightbus
from lightbus import BusPath
from lightbus.transports.redis import StreamUse

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
    return lightbus.RedisEventTransport(
        redis_pool=new_redis_pool(maxsize=10000),
        consumer_group_prefix="test_cg",
        consumer_name="test_consumer",
        # This used to be the default, so we still test against it here
        stream_use=StreamUse.PER_EVENT,
    )


@pytest.fixture
def redis_schema_transport(new_redis_pool, server, loop):
    """Get a redis transport backed by a running redis server."""
    logger.debug("Loop: {}".format(id(loop)))
    return lightbus.RedisSchemaTransport(redis_pool=new_redis_pool(maxsize=10000))


@pytest.fixture
def bus(
    loop, redis_rpc_transport, redis_result_transport, redis_event_transport, redis_schema_transport
):
    """Get a redis transport backed by a running redis server."""
    return lightbus.create(
        rpc_transport=redis_rpc_transport,
        result_transport=redis_result_transport,
        event_transport=redis_event_transport,
        schema_transport=redis_schema_transport,
        loop=loop,
    )


@pytest.fixture(name="fire_dummy_events")
def fire_dummy_events_fixture(bus):

    async def fire_dummy_events(total, initial_delay=0.1):
        await asyncio.sleep(initial_delay)
        for x in range(0, total):
            await bus.my.dummy.my_event.fire_async(field=x)
        logger.warning("TEST: fire_dummy_events() completed")

    return fire_dummy_events


@pytest.fixture
def new_bus(loop, new_redis_pool, server):

    async def wrapped():
        rpc_pool = await create_redis_pool(server.tcp_address, loop=loop, maxsize=1000)
        result_pool = await create_redis_pool(server.tcp_address, loop=loop, maxsize=1000)
        event_pool = await create_redis_pool(server.tcp_address, loop=loop, maxsize=1000)
        schema_pool = await create_redis_pool(server.tcp_address, loop=loop, maxsize=1000)

        return await lightbus.create_async(
            rpc_transport=lightbus.RedisRpcTransport(redis_pool=rpc_pool),
            result_transport=lightbus.RedisResultTransport(redis_pool=result_pool),
            event_transport=lightbus.RedisEventTransport(
                redis_pool=event_pool,
                consumer_group_prefix="test_cg",
                consumer_name="test_consumer",
            ),
            schema_transport=lightbus.RedisSchemaTransport(redis_pool=schema_pool),
            loop=loop,
        )

    return wrapped


@pytest.fixture
def get_total_redis_connections(redis_client):

    async def _get_total_redis_connections():
        return int((await redis_client.info())["clients"]["connected_clients"])

    return _get_total_redis_connections
