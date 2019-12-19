import asyncio
import logging

import pytest

import lightbus
import lightbus.creation
import lightbus.transports.redis.event
import lightbus.transports.redis.result
import lightbus.transports.redis.rpc
import lightbus.transports.redis.schema
from lightbus.exceptions import BusAlreadyClosed
from lightbus.transports.redis.event import StreamUse

logger = logging.getLogger(__name__)


@pytest.fixture
async def redis_rpc_transport(new_redis_pool, loop):
    return lightbus.transports.redis.rpc.RedisRpcTransport(
        redis_pool=await new_redis_pool(maxsize=10000)
    )


@pytest.fixture
async def redis_result_transport(new_redis_pool, loop):
    return lightbus.transports.redis.result.RedisResultTransport(
        redis_pool=await new_redis_pool(maxsize=10000)
    )


@pytest.yield_fixture
async def redis_event_transport(new_redis_pool, loop):
    transport = lightbus.transports.redis.event.RedisEventTransport(
        redis_pool=await new_redis_pool(maxsize=10000),
        service_name="test_service",
        consumer_name="test_consumer",
        # This used to be the default, so we still test against it here
        stream_use=StreamUse.PER_EVENT,
    )
    yield transport
    await transport.close()


@pytest.fixture
async def redis_schema_transport(new_redis_pool, loop):
    return lightbus.transports.redis.schema.RedisSchemaTransport(
        redis_pool=await new_redis_pool(maxsize=10000)
    )


@pytest.yield_fixture
async def bus(new_bus):
    bus = new_bus()

    yield bus

    try:
        await bus.client.close_async()
    except BusAlreadyClosed:
        pass


@pytest.fixture(name="fire_dummy_events")
def fire_dummy_events_fixture(bus):
    async def fire_dummy_events(total, initial_delay=0.1):
        await asyncio.sleep(initial_delay)
        for x in range(0, total):
            await bus.my.dummy.my_event.fire_async(field=str(x))
        logger.warning("TEST: fire_dummy_events() completed")

    return fire_dummy_events


@pytest.fixture
def get_total_redis_connections(redis_client):
    async def _get_total_redis_connections():
        info = await redis_client.info()
        return int(info["clients"]["connected_clients"])

    return _get_total_redis_connections
