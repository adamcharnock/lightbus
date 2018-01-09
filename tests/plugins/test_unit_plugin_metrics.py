import asyncio
import pytest

from lightbus.api import registry, Api, Event
from lightbus.bus import BusNode
from lightbus.plugins import manually_set_plugins
from lightbus.plugins.metrics import MetricsPlugin
from lightbus.transports.debug import DebugEventTransport


pytestmark = pytest.mark.unit


class TestApi(Api):
    my_event = Event(arguments=[])

    def my_method(self, f=None):
        return 'value'

    class Meta:
        name = 'example.test'


@pytest.fixture
def dummy_events(mocker, dummy_bus: BusNode):
    """Get events sent on the dummy bus"""
    mocker.spy(dummy_bus.bus_client.event_transport, 'send_event')

    def get_events():
        return [
            args[0]
            for args, kwargs
            in dummy_bus.bus_client.event_transport.send_event.call_args_list
        ]

    return get_events


@pytest.mark.run_loop
async def test_remote_rpc_call(dummy_bus: BusNode, dummy_events):
    async def dummy_coroutine(*args, **kwargs):
        pass

    # Setup the bus and do the call
    manually_set_plugins(plugins={'metrics': MetricsPlugin()})
    registry.add(TestApi())
    await dummy_bus.example.test.my_method.call_async(f=123)

    # What events were fired?
    event_messages = dummy_events()
    assert len(event_messages) == 2

    # rpc_call_sent
    assert event_messages[0].api_name == 'internal.metrics'
    assert event_messages[0].event_name == 'rpc_call_sent'
    assert event_messages[0].kwargs.pop('timestamp')
    assert event_messages[0].kwargs == {
        'process_name': 'foo',
        'api_name': 'example.test',
        'procedure_name': 'my_method',
        'rpc_id': 'rpc_id',
        'kwargs': {'f': 123},
    }

    # rpc_response_received
    assert event_messages[1].api_name == 'internal.metrics'
    assert event_messages[1].event_name == 'rpc_response_received'
    assert event_messages[1].kwargs.pop('timestamp')
    assert event_messages[1].kwargs == {
        'process_name': 'foo',
        'api_name': 'example.test',
        'procedure_name': 'my_method',
        'rpc_id': 'rpc_id',
    }


