import asyncio

import logging
from asyncio.futures import CancelledError

from random import random

import pytest
import lightbus
from lightbus.exceptions import SuddenDeathException
from lightbus.utilities import handle_aio_exceptions
from tests.dummy_api import DummyApi


@pytest.mark.run_loop  # TODO: Have test repeat a few times
async def test_event(bus: lightbus.BusNode, redis_pool, caplog):
    """Full rpc call integration test"""
    caplog.set_level(logging.INFO)

    event_ok_ids = dict()
    event_mayhem_ids = dict()

    async def listener(**kwargs):
        call_id = int(kwargs['field'])
        if random() < 0.2:
            # Cause some mayhem
            event_mayhem_ids.setdefault(call_id, 0)
            event_mayhem_ids[call_id] += 1
            raise SuddenDeathException()
        else:
            event_ok_ids.setdefault(call_id, 0)
            event_ok_ids[call_id] += 1

    async def co_fire_event():
        await asyncio.sleep(0.1)

        for x in range(0, 100, 10):
            logging.info("Firing next batch starting at {}".format(x))
            await asyncio.gather(*[
                bus.my.dummy.my_event.fire_async(field=x + y)
                for y in range(0, 10)
            ])

    async def co_listen_for_events():
        await bus.my.dummy.my_event.listen_async(listener)
        while len(event_ok_ids) < 100:
            logging.info("Listening. Seen {} so far".format(len(event_ok_ids)))
            await bus.bus_client._consume_events_once()

    done, pending = await asyncio.wait(
        [
            handle_aio_exceptions(co_fire_event()),
            handle_aio_exceptions(co_listen_for_events()),
        ],
        # return_when=asyncio.FIRST_COMPLETED,
        timeout=10
    )

    for task in list(pending):
        import pdb; pdb.set_trace()
        task.cancel()
        try:
            await task
        except CancelledError:
            pass

    assert len(event_ok_ids) == 100
    assert set(event_ok_ids.keys()) == set(range(0, 100))
