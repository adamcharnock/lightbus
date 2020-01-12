import asyncio
import logging
from asyncio import CancelledError

import pytest

import lightbus
import lightbus.path
from lightbus import EventMessage
from lightbus.utilities.async_tools import cancel
from tests.conftest import Worker

pytestmark = pytest.mark.reliability

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_listener_failures(
    bus: lightbus.path.BusPath, new_bus, caplog, redis_client, dummy_api, worker: Worker
):
    """Keep killing bus clients and check that we don't loose any events regardless"""

    caplog.set_level(logging.ERROR)
    event_ok_ids = dict()
    history = []

    async def listener(event_message: EventMessage, field, **kwargs):
        call_id = int(field)
        event_ok_ids.setdefault(call_id, 0)
        event_ok_ids[call_id] += 1
        await asyncio.sleep(0.1)

    # Put a lot of events onto the bus (we'll pull them off shortly)
    bus.client.register_api(dummy_api)
    for n in range(0, 50):
        await bus.my.dummy.my_event.fire_async(field=str(n))

    # Now pull the events off, and sometimes kill a worker early.
    # We kill 20% of listeners, so run 20% extra workers (we don't kill
    # any listeners in that extra 20% because these are just mop-up)
    for n in range(0, int(50 * 1.2)):
        cursed_bus = new_bus()
        cursed_bus.my.dummy.my_event.listen(
            listener, listener_name="test", bus_options={"since": "0"}
        )

        async with worker(cursed_bus):
            logger.debug(f"Worker {n}")
            await asyncio.sleep(0.05)
            if n % 5 == 0 and n < 50:
                # Cancel 1 in every 5 attempts at handling the event
                tasks = cursed_bus.client.event_client._event_listener_tasks
                await cancel(list(tasks)[0])
            await asyncio.sleep(0.15)

    await asyncio.sleep(0.1)

    info = await redis_client.xinfo_groups(stream="my.dummy.my_event:stream")

    duplicate_calls = [n for n, v in event_ok_ids.items() if v > 1]
    assert len(event_ok_ids) == 50, event_ok_ids
    assert len(duplicate_calls) > 0

    assert (
        len(info) == 1
    ), "There should only be one consumer group which was reused by every listener above"
    assert info[0][b"pending"] == 0
