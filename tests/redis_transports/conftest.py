import asyncio
import logging
from contextlib import contextmanager, asynccontextmanager

import pytest
from aioredis import create_redis_pool

import lightbus
import lightbus.creation
import lightbus.transports.redis.event
import lightbus.transports.redis.result
import lightbus.transports.redis.rpc
import lightbus.transports.redis.schema
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
from lightbus.exceptions import BusAlreadyClosed
from lightbus.path import BusPath
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


class Worker:
    def __init__(self, bus: BusPath):
        self.bus: BusPath = bus

    async def start(self):
        await self.bus.client.start_server()

    async def stop(self):
        try:
            await self.bus.client.stop_server()
        finally:
            # Stop server can raise any queued exceptions that had
            # not previously been raised, so make sure we
            # do the close by wrapping it in a finally
            try:
                await self.bus.client.close_async()
            except BusAlreadyClosed:
                pass

    def __call__(self, raise_errors=True):
        @asynccontextmanager
        async def worker_context():
            await self.start()

            yield

            try:
                await self.stop()
            except Exception as e:
                if raise_errors:
                    raise
                else:
                    logger.error(e)

        return worker_context()


@pytest.yield_fixture
async def worker(new_bus):
    yield Worker(new_bus())


@pytest.fixture(name="fire_dummy_events")
def fire_dummy_events_fixture(bus):
    async def fire_dummy_events(total, initial_delay=0.1):
        await asyncio.sleep(initial_delay)
        for x in range(0, total):
            await bus.my.dummy.my_event.fire_async(field=str(x))
        logger.warning("TEST: fire_dummy_events() completed")

    return fire_dummy_events


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
        )
        return bus
    return _new_bus
# fmt: on


@pytest.fixture
def get_total_redis_connections(redis_client):
    async def _get_total_redis_connections():
        info = await redis_client.info()
        return int(info["clients"]["connected_clients"])

    return _get_total_redis_connections
