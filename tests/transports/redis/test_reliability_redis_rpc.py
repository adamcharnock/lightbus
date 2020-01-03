import asyncio

import logging
import resource
from asyncio import CancelledError

import pytest

import lightbus
import lightbus.path
from lightbus.exceptions import LightbusTimeout


pytestmark = pytest.mark.reliability

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_many_calls_and_clients(bus: lightbus.path.BusPath, new_bus, caplog, dummy_api, loop):
    caplog.set_level(logging.WARNING)
    loop.slow_callback_duration = 0.01
    results = []

    async def do_single_call(client_bus):
        nonlocal results
        result = await client_bus.my.dummy.my_proc.call_async(field="x")
        results.append(result)

    client_buses = [new_bus() for n in range(0, 100)]
    server_buses = [new_bus() for n in range(0, 10)]

    for server_bus in server_buses:
        server_bus.client.register_api(dummy_api)
        await server_bus.client.consume_rpcs(apis=[dummy_api])

    # Perform a lot of calls in parallel
    await asyncio.gather(*[do_single_call(client_bus) for client_bus in client_buses])

    for bus_ in server_buses + client_buses:
        await bus_.client.close_async()

    assert len(results) == 100


@pytest.mark.asyncio
async def test_timeouts(bus: lightbus.path.BusPath, new_bus, caplog, dummy_api, loop):
    caplog.set_level(logging.ERROR)
    loop.slow_callback_duration = 0.01
    results = []

    async def do_single_call(n, client_bus):
        nonlocal results
        try:
            result = await client_bus.my.dummy.random_death.call_async(
                n=n, death_every=20, bus_options={"timeout": 1}
            )
            results.append(result)
        except LightbusTimeout:
            results.append(None)

    client_buses = [new_bus() for n in range(0, 100)]
    # Create a lot of servers so we have enough to handle all the RPCs before the timeout
    server_buses = [new_bus() for n in range(0, 20)]

    for server_bus in server_buses:
        server_bus.client.register_api(dummy_api)
        await server_bus.client.consume_rpcs(apis=[dummy_api])

    # Perform a lot of calls in parallel
    await asyncio.gather(
        *[do_single_call(n, client_bus) for n, client_bus in enumerate(client_buses)]
    )

    for bus_ in server_buses + client_buses:
        await bus_.client.close_async()

    total_successful = len([r for r in results if r is not None])
    total_timeouts = len([r for r in results if r is None])
    assert len(results) == 100
    assert total_timeouts == 5
    assert total_successful == 95
