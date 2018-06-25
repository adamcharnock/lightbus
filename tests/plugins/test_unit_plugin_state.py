import asyncio
import pytest

from lightbus.api import registry, Api, Event
from lightbus.bus import BusNode
from lightbus.plugins.state import StatePlugin
from lightbus.utilities.async import cancel

pytestmark = pytest.mark.unit


class TestApi(Api):
    my_event = Event(parameters=[])

    class Meta:
        name = "example.test"


@pytest.mark.run_loop
async def test_before_server_start(dummy_bus: BusNode, loop, get_dummy_events):
    registry.add(TestApi())
    listener = await dummy_bus.example.test.my_event.listen_async(lambda *a, **kw: None)
    await asyncio.sleep(0.1)  # Give the bus a moment to kick up the listener

    state_plugin = StatePlugin(service_name="foo", process_name="bar")
    state_plugin.ping_enabled = False
    await state_plugin.before_server_start(client=dummy_bus.client)
    await cancel(listener)

    dummy_events = get_dummy_events()
    assert len(dummy_events) == 1
    event_message = dummy_events[0]

    assert event_message.api_name == "internal.state"
    assert event_message.event_name == "server_started"

    assert event_message.kwargs["api_names"] == ["example.test"]
    assert event_message.kwargs["listening_for"] == ["example.test.my_event"]
    assert event_message.kwargs["metrics_enabled"] == False
    assert event_message.kwargs["ping_enabled"] == False
    assert event_message.kwargs["ping_interval"] == 60
    assert event_message.kwargs["service_name"] == "foo"
    assert event_message.kwargs["process_name"] == "bar"


@pytest.mark.run_loop
async def test_ping(dummy_bus: BusNode, loop, get_dummy_events):
    # We check the pings message contains a list of registries, so register one
    registry.add(TestApi())
    # Likewise for event listeners
    await dummy_bus.example.test.my_event.listen_async(lambda *a, **kw: None)

    # Let the state plugin send a ping then cancel it
    state_plugin = StatePlugin(service_name="foo", process_name="bar")
    state_plugin.ping_interval = 0.1
    task = asyncio.ensure_future(state_plugin._send_ping(client=dummy_bus.client), loop=loop)
    await asyncio.sleep(0.15)
    await cancel(task)

    dummy_events = get_dummy_events()
    assert len(dummy_events) == 1
    event_message = dummy_events[0]

    assert event_message.api_name == "internal.state"
    assert event_message.event_name == "server_ping"

    assert event_message.kwargs["api_names"] == ["example.test"]
    assert event_message.kwargs["listening_for"] == ["example.test.my_event"]
    assert event_message.kwargs["metrics_enabled"] == False
    assert event_message.kwargs["ping_enabled"] == True
    assert event_message.kwargs["ping_interval"] == 0.1
    assert event_message.kwargs["service_name"] == "foo"
    assert event_message.kwargs["process_name"] == "bar"


@pytest.mark.run_loop
async def test_after_server_stopped(dummy_bus: BusNode, loop, get_dummy_events):
    registry.add(TestApi())
    listener = await dummy_bus.example.test.my_event.listen_async(lambda *a, **kw: None)

    plugin = StatePlugin(service_name="foo", process_name="bar")
    plugin._ping_task = asyncio.Future()
    await plugin.after_server_stopped(client=dummy_bus.client)

    # Give any pending coroutines coroutines a moment to be awaited
    await asyncio.sleep(0.001)

    dummy_events = get_dummy_events()
    assert len(dummy_events) == 1
    event_message = dummy_events[0]

    assert event_message.api_name == "internal.state"
    assert event_message.event_name == "server_stopped"
    assert event_message.kwargs["service_name"] == "foo"
    assert event_message.kwargs["process_name"] == "bar"

    await cancel(listener)
