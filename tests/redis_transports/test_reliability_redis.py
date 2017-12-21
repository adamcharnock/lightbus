import asyncio

import logging
from asyncio.futures import CancelledError

from random import random

import pytest
import lightbus
from lightbus.exceptions import SuddenDeathException
from lightbus.utilities import handle_aio_exceptions
from tests.dummy_api import DummyApi


@pytest.mark.run_loop
async def test_event(bus: lightbus.BusNode, redis_pool, caplog):
    """Full rpc call integration test"""
    caplog.set_level(logging.WARNING)

    event_kwargs_ok = []
    event_kwargs_mayhem = []

    async def listener(**kwargs):
        if random() < 1:
            # Cause some mayhem
            event_kwargs_mayhem.append(kwargs)
            raise SuddenDeathException()
        else:
            event_kwargs_ok.append(kwargs)

    async def co_fire_event():
        await asyncio.sleep(0.1)
        await asyncio.gather(*[
            bus.my.dummy.my_event.fire_async(field='Hello! 😎')
            for x in range(0, 100)
        ])

    async def co_listen_for_events():
        await bus.my.dummy.my_event.listen_async(listener)
        await bus.bus_client.consume_events()

    done, pending = await asyncio.wait(
        [
            handle_aio_exceptions(co_fire_event()),
            handle_aio_exceptions(co_listen_for_events()),
        ],
        return_when=asyncio.FIRST_COMPLETED,
        timeout=10
    )

    for task in list(pending):
        task.cancel()
        try:
            await task
        except CancelledError:
            pass

    assert len(event_kwargs_ok) + len(event_kwargs_mayhem) == 100
