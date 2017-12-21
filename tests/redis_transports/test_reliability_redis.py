import asyncio

import logging
import pytest
import lightbus
from lightbus.utilities import handle_aio_exceptions
from tests.dummy_api import DummyApi


@pytest.mark.run_loop
@pytest.mark.skip
async def test_event(bus: lightbus.BusNode, redis_pool, caplog):
    """Full rpc call integration test"""
    caplog.set_level(logging.WARNING)

    received_kwargs = []

    async def listener(**kwargs):
        received_kwargs.append(kwargs)

    async def co_fire_event():
        await asyncio.sleep(0.1)
        await asyncio.gather(*[
            bus.my.dummy.my_event.fire_async(field='Hello! 😎')
            for x in range(0, 100)
        ])

    async def co_listen_for_events():
        await bus.my.dummy.my_event.listen_async(listener)
        async with bus.bus_client.consume_events():
            pass

    async def co_cause_mayhem():
        # This is not how we should cause mayhem. But what mayhem
        # should we cause and how should we do it?
        while True:
            await redis_pool.flushdb()
            await asyncio.sleep(0.2)

    done, pending = await asyncio.wait(
        [
            handle_aio_exceptions(co_fire_event()),
            handle_aio_exceptions(co_listen_for_events()),
            handle_aio_exceptions(co_cause_mayhem()),
        ],
        return_when=asyncio.FIRST_COMPLETED,
        timeout=10
    )
    for task in list(done) + list(pending):
        task.cancel()
        await task

    assert len(received_kwargs) == 100
