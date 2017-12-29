from lightbus.api import registry, Api, Event
from lightbus.bus import BusNode
from lightbus.internal_apis import LightbusStateApi
from lightbus.plugins.state import StatePlugin
from lightbus.transports.debug import DebugEventTransport


class TestApi(Api):
    my_event = Event(arguments=[])

    class Meta:
        name = 'example.test'


def test_before_server_start(dummy_bus: BusNode, loop, mocker):
    m = mocker.patch.object(DebugEventTransport, 'send_event')

    registry.add(TestApi())
    dummy_bus.example.test.my_event.listen(lambda: None)

    StatePlugin().before_server_start(bus_client=dummy_bus.bus_client, loop=loop)
    assert m.called
    (event_message, ), _ = m.call_args
    assert event_message.api_name == 'internal.state'
    assert event_message.event_name == 'server_started'

    assert event_message.kwargs['api_names'] == ['example.test']
    assert event_message.kwargs['listening_for'] == ['example.test.my_event']
    assert event_message.kwargs['metrics_enabled'] == False


def test_after_server_stopped(dummy_bus: BusNode, loop, mocker):
    async def dummy_coroutine(*args, **kwargs):
        pass
    m = mocker.patch.object(DebugEventTransport, 'send_event', return_value=dummy_coroutine())

    registry.add(TestApi())
    dummy_bus.example.test.my_event.listen(lambda: None)

    StatePlugin().after_server_stopped(bus_client=dummy_bus.bus_client, loop=loop)
    assert m.called
    (event_message, ), _ = m.call_args
    assert event_message.api_name == 'internal.state'
    assert event_message.event_name == 'server_stopped'
