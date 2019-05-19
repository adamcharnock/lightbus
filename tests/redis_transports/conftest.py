import asyncio
import logging

import pytest
from aioredis import create_redis_pool

import lightbus
import lightbus.creation
from lightbus import (
    RedisRpcTransport,
    RedisSchemaTransport,
    RedisResultTransport,
    RedisEventTransport,
)
from lightbus.config.structure import (
    RootConfig,
    ApiConfig,
    RpcTransportSelector,
    ResultTransportSelector,
    EventTransportSelector,
    SchemaTransportSelector,
    SchemaConfig,
    BusConfig,
)
from lightbus.path import BusPath
from lightbus.transports.redis import StreamUse

logger = logging.getLogger(__name__)


@pytest.fixture
async def redis_rpc_transport(new_redis_pool, loop):
    return lightbus.RedisRpcTransport(redis_pool=await new_redis_pool(maxsize=10000))


@pytest.fixture
async def redis_result_transport(new_redis_pool, loop):
    return lightbus.RedisResultTransport(redis_pool=await new_redis_pool(maxsize=10000))


@pytest.yield_fixture
async def redis_event_transport(new_redis_pool, loop):
    transport = lightbus.RedisEventTransport(
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
    return lightbus.RedisSchemaTransport(redis_pool=await new_redis_pool(maxsize=10000))


@pytest.yield_fixture
def bus(loop, redis_server_url):
    # fmt: off
    bus = lightbus.creation.create(
        config=RootConfig(
            apis={
                'default': ApiConfig(
                    rpc_transport=RpcTransportSelector(redis=RedisRpcTransport.Config(url=redis_server_url)),
                    result_transport=ResultTransportSelector(redis=RedisResultTransport.Config(url=redis_server_url)),
                    event_transport=EventTransportSelector(redis=RedisEventTransport.Config(url=redis_server_url)),
                )
            },
            bus=BusConfig(
                schema=SchemaConfig(
                    transport=SchemaTransportSelector(redis=RedisSchemaTransport.Config(url=redis_server_url)),
                )
            )
        ),
        plugins=[],
    )
    # fmt: on
    yield bus
    bus.client.close()


@pytest.fixture(name="fire_dummy_events")
def fire_dummy_events_fixture(bus):
    async def fire_dummy_events(total, initial_delay=0.1):
        await asyncio.sleep(initial_delay)
        for x in range(0, total):
            await bus.my.dummy.my_event.fire_async(field=str(x))
        logger.warning("TEST: fire_dummy_events() completed")

    return fire_dummy_events


@pytest.fixture
def new_bus(loop, redis_server_config):
    async def wrapped():
        rpc_pool = await create_redis_pool(**redis_server_config, loop=loop, maxsize=1000)
        result_pool = await create_redis_pool(**redis_server_config, loop=loop, maxsize=1000)
        event_pool = await create_redis_pool(**redis_server_config, loop=loop, maxsize=1000)
        schema_pool = await create_redis_pool(**redis_server_config, loop=loop, maxsize=1000)

        return await lightbus.creation.create_async(
            rpc_transport=lightbus.RedisRpcTransport(redis_pool=rpc_pool),
            result_transport=lightbus.RedisResultTransport(redis_pool=result_pool),
            event_transport=lightbus.RedisEventTransport(
                redis_pool=event_pool, service_name="test_service", consumer_name="test_consumer"
            ),
            schema_transport=lightbus.RedisSchemaTransport(redis_pool=schema_pool),
        )

    return wrapped


@pytest.fixture
def get_total_redis_connections(redis_client):
    async def _get_total_redis_connections():
        return int((await redis_client.info())["clients"]["connected_clients"])

    return _get_total_redis_connections
