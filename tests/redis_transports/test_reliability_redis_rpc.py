import asyncio

import logging
from asyncio.futures import CancelledError

import pytest
from aioredis import create_redis_pool

import lightbus
from lightbus.exceptions import SuddenDeathException, LightbusTimeout


pytestmark = pytest.mark.reliability


@pytest.mark.run_loop
async def test_timeouts(bus: lightbus.BusNode, caplog, dummy_api, loop):
    caplog.set_level(logging.WARNING)
    loop.slow_callback_duration = 0.01
    results = []

    async def do_single_call(n):
        nonlocal results
        try:
            result = await bus.my.dummy.random_death.call_async(n=n, death_probability=0.5)
            results.append(result)
        except LightbusTimeout:
            results.append(None)

    async def co_call_rpc():
        await asyncio.sleep(0.1)
        fut = asyncio.gather(*[do_single_call(n) for n in range(0, 100)])
        await fut
        return fut.result()

    async def co_consume_rpcs():
        return await bus.bus_client.consume_rpcs(apis=[dummy_api])

    (call_task,), (consume_task,) = await asyncio.wait(
        [co_call_rpc(), co_consume_rpcs()], return_when=asyncio.FIRST_COMPLETED
    )
    call_task.result()
    consume_task.cancel()
    try:
        await consume_task
        consume_task.result()
    except CancelledError:
        pass

    total_successful = len([r for r in results if r is not None])
    total_timeouts = len([r for r in results if r is None])
    assert len(results) == 100
    assert total_successful > 0
    assert total_timeouts > 0


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
