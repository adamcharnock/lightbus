import asyncio

import logging
from asyncio.futures import CancelledError

from random import random

import pytest
import lightbus
from lightbus.exceptions import SuddenDeathException
from lightbus.utilities import handle_aio_exceptions

pytestmark = pytest.mark.reliability

logger = logging.getLogger(__name__)


@pytest.mark.run_loop  # TODO: Have test repeat a few times
async def test_random_failures(bus: lightbus.BusNode, caplog, fire_dummy_events, dummy_api, mocker):
    # Use test_history() (below) to repeat any cases which fail
    caplog.set_level(logging.WARNING)

    event_ok_ids = dict()
    history = []

    async def listener(field, **kwargs):
        call_id = field
        event_ok_ids.setdefault(call_id, 0)
        event_ok_ids[call_id] += 1
        await asyncio.sleep(0.01)

    fire_task = asyncio.ensure_future(handle_aio_exceptions(fire_dummy_events(total=100, initial_delay=0.1)))

    for _ in range(0, 20):
        logging.warning('TEST: Still waiting for events to finish. {} so far'.format(len(event_ok_ids)))
        for _ in range(0, 5):
            listen_task = asyncio.ensure_future(handle_aio_exceptions(
                bus.my.dummy.my_event.listen_async(listener)
            ))
            await asyncio.sleep(0.2)
            listen_task.cancel()
            await listen_task

        if len(event_ok_ids) == 100:
            logging.warning('TEST: Events finished')
            break

    # Cleanup the tasks
    fire_task.cancel()
    try:
        await fire_task
    except CancelledError:
        pass

    logger.warning("History: {}".format(','.join('{}{}'.format(*x) for x in history)))

    assert set(event_ok_ids.keys()) == set(range(0, 100))

    duplicate_calls = sum([n - 1 for n in event_ok_ids.values()])
    assert duplicate_calls > 0


@pytest.mark.run_loop  # TODO: Have test repeat a few times
async def test_first_event_fails(bus: lightbus.BusNode, caplog, fire_dummy_events, listen_for_events, dummy_api):
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
async def test_last_event_fails(bus: lightbus.BusNode, caplog, fire_dummy_events, listen_for_events, dummy_api):
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


@pytest.mark.skip
@pytest.mark.run_loop
async def test_history(bus: lightbus.BusNode, caplog, fire_dummy_events, listen_for_events, dummy_api):
    # Takes the history output of test_random_failures() in order
    # to retest a specific case that failed
    history = "0S,1E,1S,2E,1S,2E,1S,2S,3S,4E,1S,2E,1E,1S,2S,3S,4S,5E,1S,2E,1S,2S,3S,4S,5E,1E,1S,2S,3E,1S,2S,3E,1S,2S,3S,4S,5S,6S,7E,1S,2E,1S,2S,3S,4S,5S,6S,7S,8E,1S,2E,1S,2S,3S,4E,1S,2S,3S,4E,1E,1E,1S,2E,1S,2S,3S,4S,5S,6S,7S,8S,9S,10E,1S,2E,1E,1E,1S,2E,1S,2E,1E,1E,1S,2E,1S,2E,1S,2S,3S,4S,5S,6S,7S,8S,9E,1S,2E,1S,2S,3S,4S,5S,6S,7E,1S,2E,1E,1S,2S,3S,4E,1S,2E,1S,2E,1S,2S,3S,4S,5S,6E,1S,2E,1E,1S,2S,3S,4S,5E,1S,2E,1E,1S,2S,3S,4E,1S,2S,3S,4S,5E,1S,2E,1E,1S,2S,3S,4S,5S,6E,1S,2S,3E,1S,2E,1S,2E,1S,2E,1S,2E,1E,1S,2S,3S,4E,1S,2S,3S,4S,5E,1S,2E,1S,2S,3E,1S,2S,3S,4S,5S,6E,1S,2S,3S,4S,5S,6E,1E,1S,2E,1E,1S,2S,3S,4S,5E,1S,2E,1S,2S,3E,1S,2S,3S,4S,5S,6E,1E,1S,2S,3S,4E,1E,1S,2S,3S,4S,5E,1S,2E,1E,1S,2S,3S,4E,1S,2S,3S,4S,5S,6S,7E,1S,2S,3S,4S,5S,6S,7S,8S,9S,10E,1S,2S,3S,4S,5S,6E,1S,2S,3S,4S,5E,1E,1E,1S,2E,1E,1E,1S,2S,3S,4S,5E,1E,1E,1S,2S,3S,4S,5E,1S,2S,3E,1S,2S,3E,1S,2S,3E,1S,2S,3S,4E,1E,1E,1S,2S,3S,4S,5E,1S,2S,3E,1S,2E,1S,2S,3S,4S,5S,6S,7S,8S,9S,10E,1S,2S,3S,4E,1S,2S,3S,4E,1S,2S,3S,4S,5E,1E,1S,2E,1S,2E,1S,2S,3E,1E,1S,2E,1S,2S,3S,4S,5E,1S,2S,3S,4S,5S,6E,1S,2S,3S,4S,5S,6E,1S,2E,1S,2S,3S,4S,5E,1E,1S,2S,3E,1E,1S,2E,1S,2S,3S,4S,5S,6E,1S,2S,3S,4S,5S,6S,7S,8S,9E,1S,2S,3S,4S,5E,1S,2S,3E,1E,1S,2E,1S,2S,3S,4S,5E,1E,1S,2E,1S,2S,3S,4S,5S,6S,7S,8E,1S,2E,1S,2S,3E,1S,2E,1E,1S,2E,1S,2S,3S,4S,5S,6S,7E,1E,1E,1E,1S,2S,3S,4S,5E,1S,2S,3E,1E,1S,2S,3S,4E,1S,2S,3S,4S,5S,6E,1S,2S,3S,4S,5S,6E,1S,2E,1S,2S,3S,4S,5E,1S,2S,3E,1S,2S,3S,4E,1S,2E,1S,2S,3S,4S,5S,6S,7S,8S,9S,10S,11E,11S,12S,13E,11S,12E,11S,12E,11E,11S,12S,13S,14S,15E,11E,11S,12S,13S,14S,15S,16S,17E,11S,12E,11S,12S,13S,14S,15S,16E,11S,12S,13S,14S,15S,16E,11E,11E,11S,12S,13S,14E,11S,12S,13S,14E,11S,12E,11E,11E,11E,11S,12S,13S,14S,15S,16S,17S,18S,19S,20S,21E,21S,22S,23S,24S,25S,26E,21S,22S,23S,24E,21S,22S,23S,24S,25S,26E,21S,22S,23S,24S,25S,26S,27S,28E,21S,22S,23S,24S,25S,26S,27S,28S,29S,30S,31E,31S,32S,33S,34S,35E,31S,32S,33E,31S,32S,33E,31S,32S,33E,31S,32E,31E,31S,32E,31E,31S,32E,31E,31S,32S,33E,31E,31S,32S,33E,31S,32S,33S,34S,35E,31S,32S,33E,31S,32E,31S,32S,33E,31E,31S,32S,33E,31S,32E,31E,31S,32E,31S,32S,33S,34S,35S,36E,31E,31E,31S,32E,31E,31S,32S,33E,31E,31E,31S,32E,31S,32E,31S,32S,33S,34S,35E,31S,32S,33S,34S,35S,36E,31S,32S,33E,31S,32S,33S,34E,31S,32S,33S,34S,35S,36E,31S,32E,31E,31E,31S,32S,33S,34E,31S,32S,33S,34E,31S,32E,31S,32S,33S,34E,31E,31E,31S,32E,31S,32S,33E,31E,31S,32S,33S,34S,35E,31E,31S,32S,33S,34S,35S,36E,31S,32S,33E,31S,32E,31S,32E,31S,32S,33S,34S,35S,36S,37E,31E,31S,32S,33E,31S,32S,33S,34S,35S,36E,31S,32S,33S,34S,35S,36E,31S,32S,33S,34S,35E,31E,31E,31S,32E,31E,31S,32S,33S,34E,31S,32E,31E,31S,32S,33S,34S,35E,31S,32E,31S,32S,33S,34S,35E,31S,32S,33S,34E,31S,32E,31S,32E,31S,32S,33S,34S,35S,36E,31E,31S,32S,33S,34S,35E,31E,31S,32E,31S,32S,33E,31S,32E,31E,31S,32S,33E,31S,32E,31S,32S,33S,34E,31E,31S,32E,31E,31E,31S,32S,33E,31S,32S,33S,34S,35S,36S,37S,38E,31S,32S,33S,34E,31E,31S,32S,33E,31S,32S,33S,34E,31E,31E,31S,32S,33S,34E,31S,32S,33S,34S,35S,36S,37E,31E,31S,32S,33S,34S,35S,36S,37S,38E,31S,32S,33E,31S,32E,31S,32E,31S,32E,31S,32E,31S,32S,33S,34S,35E,31S,32E,31S,32S,33S,34S,35E,31E,31E,31S,32E,31E,31S,32S,33E,31S,32E,31S,32E,31S,32S,33S,34S,35S,36E,31S,32S,33E,31S,32S,33S,34S,35E,31S,32S,33S,34S,35E,31E,31S,32S,33S,34S,35E,31E,31E,31E,31S,32S,33S,34E,31E,31S,32E,31S,32S,33S,34S,35S,36E,31E,31S,32E,31S,32E,31S,32S,33S,34E,31S,32S,33S,34S,35S,36E,31S,32E,31S,32S,33S,34E,31E,31S,32S,33S,34S,35S,36S,37S,38E,31S,32E,31S,32S,33S,34S,35S,36S,37S,38E,31E,31E,31S,32S,33S,34S,35S,36S,37S,38S,39S,40S,41E,41S,42S,43S,44S,45E,41S,42S,43S,44S,45S,46S,47S,48S,49S,50S,51E,51S,52S,53E,51S,52S,53E,51E,51E,51S,52S,53S,54S,55S,56E,51S,52E,51S,52E,51S,52E,51E,51E,51E,51S,52S,53S,54S,55S,56S,57E,51S,52S,53E,51S,52E,51S,52S,53S,54S,55S,56E,51S,52S,53S,54S,55S,56S,57S,58S,59S,60E,51E,51S,52S,53S,54S,55S,56S,57S,58S,59E,51S,52S,53E,51S,52S,53S,54E,51E,51E,51E,51S,52S,53S,54S,55S,56E,51S,52S,53S,54S,55E,51S,52S,53S,54S,55S,56E,51S,52S,53E,51S,52S,53S,54S,55E,51S,52S,53S,54S,55S,56S,57S,58S,59S,60E,51S,52E,51S,52S,53S,54S,55S,56S,57S,58S,59E,51E,51S,52S,53S,54E,51S,52S,53S,54S,55E,51S,52S,53S,54E,51E,51S,52E,51S,52S,53S,54S,55S,56S,57S,58E,51S,52S,53S,54S,55E,51S,52S,53S,54E,51S,52E,51S,52S,53S,54E,51S,52S,53S,54E,51E,51E,51S,52S,53S,54S,55S,56S,57S,58S,59S,60E,51S,52E,51S,52S,53S,54E,51E,51S,52E,51E,51S,52S,53S,54S,55S,56E,51S,52S,53S,54S,55E,51S,52E,51E,51E,51E,51S,52S,53E,51S,52S,53E,51S,52S,53E,51S,52S,53S,54S,55S,56S,57S,58S,59S,60S,61S,62E,61S,62E,61E,61E,61E,61E,61S,62E,61S,62S,63S,64S,65E,61S,62S,63S,64S,65S,66S,67S,68E,61S,62E,61S,62E,61S,62S,63E,61E,61E,61S,62S,63S,64S,65S,66S,67S,68S,69E,61S,62S,63S,64S,65S,66E,61S,62E,61E,61S,62S,63S,64S,65S,66E,61E,61S,62S,63S,64S,65E,61S,62S,63S,64E,61S,62S,63E,61S,62E,61S,62S,63S,64S,65S,66E,61S,62S,63S,64E,61S,62E,61S,62S,63S,64S,65E,61S,62S,63S,64S,65E,61E,61E,61E,61S,62S,63S,64E,61S,62S,63S,64S,65S,66S,67E,61S,62S,63S,64S,65S,66S,67S,68E,61E,61S,62S,63S,64E,61S,62S,63E,61S,62S,63S,64S,65S,66S,67S,68E,61S,62S,63S,64S,65S,66S,67S,68S,69E,61S,62S,63S,64S,65S,66E,61S,62E,61E,61S,62S,63S,64S,65S,66S,67E,61S,62E,61E,61S,62S,63S,64S,65E,61S,62E,61E,61S,62S,63S,64E,61S,62S,63S,64S,65E,61S,62S,63S,64S,65E,61E,61S,62E,61S,62S,63E,61S,62S,63S,64S,65S,66S,67E,61S,62S,63S,64S,65E,61E,61E,61S,62S,63S,64S,65E,61S,62E,61E,61E,61S,62S,63E,61S,62E,61S,62S,63E,61E,61S,62E,61S,62E,61E,61S,62E,61E,61S,62S,63S,64E,61S,62S,63E,61E,61S,62S,63S,64S,65E,61E,61S,62S,63E,61S,62S,63E,61E,61E,61E,61S,62S,63E,61E,61E,61S,62S,63E,61E,61S,62S,63S,64E,61E,61E,61E,61E,61E,61S,62E,61E,61S,62S,63S,64E,61S,62S,63S,64E,61S,62S,63E,61S,62E,61S,62S,63S,64S,65E,61E,61E,61E,61S,62S,63E,61E,61E,61S,62E,61S,62S,63S,64S,65S,66E,61E,61S,62E,61S,62S,63E,61S,62E,61S,62E,61S,62S,63S,64E,61E,61S,62S,63E,61S,62S,63S,64S,65S,66E,61S,62S,63S,64S,65S,66S,67E,61S,62S,63S,64S,65S,66S,67E,61S,62S,63E,61S,62E,61E,61S,62S,63S,64S,65E,61S,62E,61E,61S,62S,63S,64S,65E,61E,61E,61S,62S,63E,61S,62S,63S,64S,65E,61S,62S,63S,64S,65S,66S,67E,61S,62S,63S,64S,65S,66E,61E,61E,61S,62S,63E,61S,62S,63S,64S,65E,61S,62S,63E,61E,61E,61S,62E,61S,62S,63S,64E,61S,62E,61E,61S,62E,61S,62S,63S,64S,65S,66S,67E,61E,61S,62S,63S,64S,65S,66S,67E,61S,62S,63S,64E,61E,61E,61E,61S,62S,63E,61S,62S,63S,64S,65S,66S,67S,68S,69S,70E"
    history = history.split(',')
    history = [(int(h[:-1]), h[-1]) for h in history]

    caplog.set_level(logging.WARNING)

    event_ok_ids = dict()
    event_mayhem_ids = dict()

    async def listener(**kwargs):
        call_id = int(kwargs['field'])
        history_call_id, action = history.pop(0)
        assert history_call_id == call_id, "History was invalid"

        if action == 'E':
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

    # Wait until we are done handling the events (up to 5 seconds)
    for _ in range(1, 10):
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

    duplicate_calls = sum([n - 1 for n in event_ok_ids.values()])
    assert duplicate_calls > 0
    assert len(event_mayhem_ids) > 0
