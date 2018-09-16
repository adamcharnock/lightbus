import asyncio
import logging
from asyncio import CancelledError

import pytest

import lightbus
import lightbus.path
from lightbus.plugins import manually_set_plugins
from lightbus.utilities.async_tools import cancel

pytestmark = pytest.mark.reliability

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_random_failures(
    bus: lightbus.path.BusPath, caplog, fire_dummy_events, dummy_api, mocker
):
    # Use test_history() (below) to repeat any cases which fail
    caplog.set_level(logging.WARNING)

    # The metrics plugins will add too much overhead to this test
    manually_set_plugins({})

    event_ok_ids = dict()
    history = []

    async def listener(event_message, field, **kwargs):
        call_id = field
        event_ok_ids.setdefault(call_id, 0)
        event_ok_ids[call_id] += 1
        await asyncio.sleep(0.01)

    fire_task = asyncio.ensure_future(fire_dummy_events(total=100, initial_delay=0.1))

    for _ in range(0, 20):
        logging.warning(
            "TEST: Still waiting for events to finish. {} so far".format(len(event_ok_ids))
        )
        for _ in range(0, 5):
            listen_task = asyncio.ensure_future(bus.my.dummy.my_event.listen_async(listener))
            await asyncio.sleep(0.2)
            await cancel(listen_task)

        if len(event_ok_ids) == 100:
            logging.warning("TEST: Events finished")
            break

    await cancel(fire_task)

    duplicate_calls = sum([n - 1 for n in event_ok_ids.values()])

    logger.warning("History: {}".format(",".join("{}{}".format(*x) for x in history)))
    logger.warning(
        "Finished with {}/100 events processed, {} duplicated calls".format(
            len(event_ok_ids), duplicate_calls
        )
    )

    assert set(event_ok_ids.keys()) == set(range(0, 100))

    assert duplicate_calls > 0
