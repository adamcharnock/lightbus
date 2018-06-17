import logging

import pytest
from aioredis import create_redis_pool

import lightbus


@pytest.mark.run_loop
async def test_create_and_destroy_redis_buses(redis_client, loop, dummy_api, new_bus, caplog):
    caplog.set_level(logging.WARNING)

    for _ in range(0, 100):
        # make a bus
        bus = await new_bus()
        # fire an event
        await bus.my.dummy.my_event.fire_async(field="a")
        # close it
        await bus.bus_client.close_async()

    info = await redis_client.info()

    assert int(info["stats"]["total_connections_received"]) >= 100
    assert int(info["clients"]["connected_clients"]) == 1


@pytest.mark.parametrize(
    "transport_class,kwargs",
    [
        (lightbus.RedisRpcTransport, {}),
        (lightbus.RedisResultTransport, {}),
        (
            lightbus.RedisEventTransport,
            {"consumer_group_prefix": "test_cg", "consumer_name": "test_consumer"},
        ),
        (lightbus.RedisSchemaTransport, {}),
    ],
    ids=["rpc", "result", "event", "schema"],
)
@pytest.mark.run_loop
async def test_create_and_destroy_redis_transports(
    transport_class, kwargs, redis_client, loop, server, caplog
):
    caplog.set_level(logging.WARNING)

    for _ in range(0, 100):
        pool = await create_redis_pool(server.tcp_address, loop=loop, maxsize=1000)
        transport = transport_class(redis_pool=pool, **kwargs)
        await transport.open()
        await transport.close()

    info = await redis_client.info()

    assert int(info["stats"]["total_connections_received"]) >= 100
    assert int(info["clients"]["connected_clients"]) == 1
