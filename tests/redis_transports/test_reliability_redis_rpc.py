import asyncio

import logging
from asyncio.futures import CancelledError

from random import random

import pytest
import lightbus
from lightbus.exceptions import SuddenDeathException, LightbusTimeout
from lightbus.utilities import handle_aio_exceptions
from tests.dummy_api import DummyApi


@pytest.mark.run_loop  # TODO: Have test repeat a few times
async def test_random_failures(bus: lightbus.BusNode, caplog, consume_rpcs, call_rpc, mocker, dummy_api, loop):
    caplog.set_level(logging.WARNING)
    loop.slow_callback_duration = 0.01

    async def co_call_rpc():
        asyncio.sleep(0.1)
        results = []
        for n in range(0, 100):
            try:
                results.append(
                    await bus.my.dummy.random_death.call_async(n=n)
                )
            except LightbusTimeout:
                results.append(None)
        return results

    async def co_consume_rpcs():
        return await bus.bus_client.consume_rpcs(apis=[dummy_api])

    (call_task, ), (consume_task, ) = await asyncio.wait([co_call_rpc(), co_consume_rpcs()], return_when=asyncio.FIRST_COMPLETED)
    consume_task.cancel()

    results = call_task.result()
    total_successful = len([r for r in results if r is not None])
    total_timeouts = len([r for r in results if r is None])
    assert len(results) == 100
    assert total_successful > 0
    assert total_timeouts > 0


@pytest.mark.run_loop  # TODO: Have test repeat a few times
async def test_first_rpc_fails(bus: lightbus.BusNode, caplog, fire_dummy_events, listen_for_events):
    caplog.set_level(logging.WARNING)

    event_ok_ids = dict()
    event_mayhem_ids = dict()

    # A listener that artificially simulates the process
    # dieing through use of the SuddenDeathException()
    async def listener(**kwargs):
        call_id = int(kwargs['field'])
        if call_id == 0 and call_id not in event_mayhem_ids:  # SIMULATE FIRST EVENT DYING ONCE
            # Cause some mayhem
            event_mayhem_ids.setdefault(call_id, 0)
            event_mayhem_ids[call_id] += 1
            raise SuddenDeathException()
        else:
            event_ok_ids.setdefault(call_id, 0)
            event_ok_ids[call_id] += 1

    done, (listen_task, ) = await asyncio.wait(
        [
            handle_aio_exceptions(fire_dummy_events(total=100, initial_delay=0.1)),
            handle_aio_exceptions(listen_for_events(listener=listener)),
        ],
        return_when=asyncio.FIRST_COMPLETED,
        timeout=10
    )

    # Wait until we are done handling the events (up to 10 seconds)
    for _ in range(1, 5):
        await asyncio.sleep(1)
        logging.warning('TEST: Still waiting for events to finish. {} so far'.format(len(event_ok_ids)))
        if len(event_ok_ids) == 100:
            logging.warning('TEST: Events finished')
            break

    # Cleanup the tasks
    listen_task.cancel()
    try:
        await listen_task
    except CancelledError:
        pass

    assert set(event_ok_ids.keys()) == set(range(0, 100))


@pytest.mark.run_loop  # TODO: Have test repeat a few times
async def test_last_rpc_fails(bus: lightbus.BusNode, caplog, fire_dummy_events, listen_for_events):
    caplog.set_level(logging.WARNING)

    event_ok_ids = dict()
    event_mayhem_ids = dict()

    # A listener that artificially simulates the process
    # dieing through use of the SuddenDeathException()
    async def listener(**kwargs):
        call_id = int(kwargs['field'])
        if call_id == 99 and call_id not in event_mayhem_ids:  # SIMULATE LAST EVENT DYING ONCE
            # Cause some mayhem
            event_mayhem_ids.setdefault(call_id, 0)
            event_mayhem_ids[call_id] += 1
            raise SuddenDeathException()
        else:
            event_ok_ids.setdefault(call_id, 0)
            event_ok_ids[call_id] += 1

    done, (listen_task, ) = await asyncio.wait(
        [
            handle_aio_exceptions(fire_dummy_events(total=100, initial_delay=0.1)),
            handle_aio_exceptions(listen_for_events(listener=listener)),
        ],
        return_when=asyncio.FIRST_COMPLETED,
        timeout=10
    )

    # Wait until we are done handling the events (up to 10 seconds)
    for _ in range(1, 5):
        await asyncio.sleep(1)
        logging.warning('TEST: Still waiting for events to finish. {} so far'.format(len(event_ok_ids)))
        if len(event_ok_ids) == 100:
            logging.warning('TEST: Events finished')
            break

    # Cleanup the tasks
    listen_task.cancel()
    try:
        await listen_task
    except CancelledError:
        pass

    assert set(event_ok_ids.keys()) == set(range(0, 100))

