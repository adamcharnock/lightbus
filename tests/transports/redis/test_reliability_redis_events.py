import asyncio
import logging
from asyncio import CancelledError

import pytest

import lightbus
import lightbus.path
from lightbus import EventMessage
from lightbus.utilities.async_tools import cancel

pytestmark = pytest.mark.reliability

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_random_failures(
    bus: lightbus.path.BusPath, new_bus, caplog, fire_dummy_events, dummy_api, mocker
):
    """Keep killing bus clients and check that we don't loose an events regardless"""

    caplog.set_level(logging.ERROR)
    event_ok_ids = dict()
    history = []

    async def listener(event_message: EventMessage, field, **kwargs):
        call_id = int(field)
        event_ok_ids.setdefault(call_id, 0)
        event_ok_ids[call_id] += 1
        await asyncio.sleep(0.03)

    # Put a lot of events onto the bus (we'll pull them off shortly)
    bus.client.register_api(dummy_api)
    for n in range(0, 100):
        await bus.my.dummy.my_event.fire_async(field=str(n))

    # Now pull the events off, and sometimes kill a worker early

    for n in range(0, 120):
        cursed_bus: lightbus.path.BusPath = new_bus(service_name="test")
        cursed_bus.my.dummy.my_event.listen(
            listener, listener_name="test", bus_options={"since": "0"}
        )
        await cursed_bus.client._setup_server()
        await asyncio.sleep(0.02)
        if n % 5 == 0:
            # Cancel 1 in every 5 attempts at handling the event
            tasks = cursed_bus.client.event_client._event_listener_tasks
            list(tasks)[0].cancel()
        await asyncio.sleep(0.05)
        await cursed_bus.client.stop_server()
        await cursed_bus.client.close_async()

    duplicate_calls = [n for n, v in event_ok_ids.items() if v > 1]

    assert len(event_ok_ids) == 100
    assert len(duplicate_calls) > 0
