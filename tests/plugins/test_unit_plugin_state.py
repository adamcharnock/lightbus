import asyncio
import pytest

from lightbus.api import registry, Api, Event
from lightbus.bus import BusNode
from lightbus.plugins.state import StatePlugin


pytestmark = pytest.mark.unit


class TestApi(Api):
    my_event = Event(arguments=[])

    class Meta:
        name = 'example.test'


@pytest.mark.run_loop
async def test_before_server_start(dummy_bus: BusNode, loop, get_dummy_events):
    registry.add(TestApi())
    await dummy_bus.example.test.my_event.listen_async(lambda **kw: None)
    await asyncio.sleep(0.1)  # Give the bus a moment to kick up the listener

    state_plugin = StatePlugin()
    state_plugin.do_ping = False
    await state_plugin.before_server_start(bus_client=dummy_bus.bus_client, loop=loop)

    dummy_events = get_dummy_events()
    assert len(dummy_events) == 1
    event_message = dummy_events[0]

    assert event_message.api_name == 'internal.state'
    assert event_message.event_name == 'server_started'

    assert event_message.kwargs['api_names'] == ['example.test']
    assert event_message.kwargs['listening_for'] == ['example.test.my_event']
    assert event_message.kwargs['metrics_enabled'] == False
    assert event_message.kwargs['ping_enabled'] == False
    assert event_message.kwargs['ping_interval'] == 60
    assert event_message.kwargs['process_name']


@pytest.mark.run_loop
async def test_ping(dummy_bus: BusNode, loop, get_dummy_events):
    # We check the pings message contains a list of registries, so register one
    registry.add(TestApi())
    # Likewise for event listeners
    await dummy_bus.example.test.my_event.listen_async(lambda **kw: None)

    # Let the state plugin send a ping then cancel it
    state_plugin = StatePlugin()
    state_plugin.ping_interval = 0.1
    task = asyncio.ensure_future(
        state_plugin._send_ping(bus_client=dummy_bus.bus_client),
        loop=loop
    )
    await asyncio.sleep(0.15)
    task.cancel()

    dummy_events = get_dummy_events()
    assert len(dummy_events) == 1
    event_message = dummy_events[0]

    assert event_message.api_name == 'internal.state'
    assert event_message.event_name == 'server_ping'

    assert event_message.kwargs['api_names'] == ['example.test']
    assert event_message.kwargs['listening_for'] == ['example.test.my_event']
    assert event_message.kwargs['metrics_enabled'] == False
    assert event_message.kwargs['ping_enabled'] == True
    assert event_message.kwargs['ping_interval'] == 0.1
    assert event_message.kwargs['process_name']


@pytest.mark.run_loop
async def test_after_server_stopped(dummy_bus: BusNode, loop, get_dummy_events):
    registry.add(TestApi())
    await dummy_bus.example.test.my_event.listen_async(lambda **kw: None)

    await StatePlugin().after_server_stopped(bus_client=dummy_bus.bus_client, loop=loop)

    dummy_events = get_dummy_events()
    assert len(dummy_events) == 1
    event_message = dummy_events[0]

    assert event_message.api_name == 'internal.state'
    assert event_message.event_name == 'server_stopped'
    assert event_message.kwargs['process_name']
