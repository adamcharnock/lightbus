import asyncio
import logging
from asyncio.futures import CancelledError

import pytest

import lightbus
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

