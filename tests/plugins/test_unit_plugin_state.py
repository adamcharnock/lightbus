import asyncio
import pytest

from lightbus.api import registry, Api, Event
from lightbus.bus import BusNode
from lightbus.plugins.state import StatePlugin
from lightbus.transports.debug import DebugEventTransport


pytestmark = pytest.mark.unit


class TestApi(Api):
    my_event = Event(arguments=[])

    class Meta:
        name = 'example.test'


@pytest.mark.run_loop
async def test_before_server_start(dummy_bus: BusNode, loop, event_consumer):
    registry.add(TestApi())
    await dummy_bus.example.test.my_event.listen_async(lambda: None)

    state_plugin = StatePlugin()
    state_plugin.do_ping = False
    await state_plugin.before_server_start(bus_client=dummy_bus.bus_client, loop=loop)

    event_messages = event_consumer()
    assert len(event_messages) == 1
    event_message = event_messages[0]

    assert event_message.api_name == 'internal.state'
    assert event_message.event_name == 'server_started'

    assert event_message.kwargs['api_names'] == ['example.test']
    assert event_message.kwargs['listening_for'] == ['example.test.my_event']
    assert event_message.kwargs['metrics_enabled'] == False
    assert event_message.kwargs['ping_enabled'] == False
    assert event_message.kwargs['ping_interval'] == 60


@pytest.mark.run_loop
async def test_ping(dummy_bus: BusNode, loop, event_consumer):
    # We check the pings message contains a list of registries, so register one
    registry.add(TestApi())
    # Likewise for event listeners
    await dummy_bus.example.test.my_event.listen_async(lambda: None)

    # Let the state plugin send a ping then cancel it
    state_plugin = StatePlugin()
    state_plugin.ping_interval = 0.1
    task = asyncio.ensure_future(
        state_plugin._send_ping(bus_client=dummy_bus.bus_client),
        loop=loop
    )
    await asyncio.sleep(0.15)
    task.cancel()

    event_messages = event_consumer()

    assert len(event_messages) == 1
    event_message = event_messages[0]

    assert event_message.api_name == 'internal.state'
    assert event_message.event_name == 'server_ping'

    assert event_message.kwargs['api_names'] == ['example.test']
    assert event_message.kwargs['listening_for'] == ['example.test.my_event']
    assert event_message.kwargs['metrics_enabled'] == False
    assert event_message.kwargs['ping_enabled'] == True
    assert event_message.kwargs['ping_interval'] == 0.1


@pytest.mark.run_loop
async def test_after_server_stopped(dummy_bus: BusNode, loop, mocker):
    async def dummy_coroutine(*args, **kwargs):
        pass
    m = mocker.patch.object(DebugEventTransport, 'send_event', return_value=dummy_coroutine())

    registry.add(TestApi())
    await dummy_bus.example.test.my_event.listen_async(lambda: None)

    await StatePlugin().after_server_stopped(bus_client=dummy_bus.bus_client, loop=loop)
    assert m.called
    (event_message, ), _ = m.call_args
    assert event_message.api_name == 'internal.state'
    assert event_message.event_name == 'server_stopped'
