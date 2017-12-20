import asyncio

import logging
import pytest
import lightbus
from lightbus.utilities import handle_aio_exceptions
from tests.dummy_api import DummyApi


# Should actually test events for reliability. We want events to
# be delivered AT LEAST ONCE. RPCs don't matter so much because
# if they fail the caller will know about it.
@pytest.mark.run_loop
async def test_event(bus: lightbus.BusNode, mocker, dummy_api, caplog):
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
        await bus.bus_client.consume_events()

    (fire_task,), (listen_task,) = await asyncio.wait(
        [
            handle_aio_exceptions(co_fire_event()),
            handle_aio_exceptions(co_listen_for_events()),
        ],
        return_when=asyncio.FIRST_COMPLETED,
        timeout=10
    )

    fire_task.cancel()
    listen_task.cancel()
    await fire_task
    await listen_task

    assert len(received_kwargs) == 100
