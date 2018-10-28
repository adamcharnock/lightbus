import asyncio
import pytest

from lightbus.api import Api, Event
from lightbus.path import BusPath
from lightbus.message import RpcMessage, EventMessage
from lightbus.plugins.metrics import MetricsPlugin
from lightbus.utilities.async_tools import cancel

pytestmark = pytest.mark.unit


class TestApi(Api):
    my_event = Event(parameters=["f"])

    def my_method(self, f=None):
        return "value"

    class Meta:
        name = "example.test"


@pytest.mark.asyncio
async def test_remote_rpc_call(dummy_bus: BusPath, get_dummy_events):
    # Setup the bus and do the call
    dummy_bus.client.plugin_registry.manually_set_plugins(
        plugins=[MetricsPlugin(service_name="foo", process_name="bar")]
    )
    dummy_bus.client.register_api(TestApi())
    await dummy_bus.example.test.my_method.call_async(f=123)

    # What events were fired?
    event_messages = get_dummy_events()
    assert len(event_messages) == 2

    # rpc_call_sent
    assert event_messages[0].api_name == "internal.metrics"
    assert event_messages[0].event_name == "rpc_call_sent"
    # Pop these next two as the values are variable
    assert event_messages[0].kwargs.pop("timestamp")
    assert event_messages[0].kwargs.pop("id")
    assert event_messages[0].kwargs == {
        "api_name": "example.test",
        "procedure_name": "my_method",
        "kwargs": {"f": 123},
        "service_name": "foo",
        "process_name": "bar",
    }

    # rpc_response_received
    assert event_messages[1].api_name == "internal.metrics"
    assert event_messages[1].event_name == "rpc_response_received"
    # Pop these next two as the values are variable
    assert event_messages[1].kwargs.pop("timestamp")
    assert event_messages[1].kwargs.pop("id")
    assert event_messages[1].kwargs == {
        "api_name": "example.test",
        "procedure_name": "my_method",
        "service_name": "foo",
        "process_name": "bar",
    }


@pytest.mark.asyncio
async def test_local_rpc_call(loop, dummy_bus: BusPath, consume_rpcs, get_dummy_events, mocker):
    rpc_transport = dummy_bus.client.transport_registry.get_rpc_transport("default")
    mocker.patch.object(
        rpc_transport,
        "_get_fake_messages",
        return_value=[
            RpcMessage(
                id="123abc", api_name="example.test", procedure_name="my_method", kwargs={"f": 123}
            )
        ],
    )

    # Setup the bus and do the call
    dummy_bus.client.plugin_registry.manually_set_plugins(
        plugins=[MetricsPlugin(service_name="foo", process_name="bar")]
    )
    dummy_bus.client.register_api(TestApi())

    task = asyncio.ensure_future(consume_rpcs(dummy_bus), loop=loop)

    # The dummy transport will fire an every every 0.1 seconds
    await asyncio.sleep(0.15)

    await cancel(task)

    event_messages = get_dummy_events()
    assert len(event_messages) == 2, event_messages

    # before_rpc_execution
    assert event_messages[0].api_name == "internal.metrics"
    assert event_messages[0].event_name == "rpc_call_received"
    assert event_messages[0].kwargs.pop("timestamp")
    assert event_messages[0].kwargs == {
        "api_name": "example.test",
        "procedure_name": "my_method",
        "id": "123abc",
        "service_name": "foo",
        "process_name": "bar",
    }

    # after_rpc_execution
    assert event_messages[1].api_name == "internal.metrics"
    assert event_messages[1].event_name == "rpc_response_sent"
    assert event_messages[1].kwargs.pop("timestamp")
    assert event_messages[1].kwargs == {
        "api_name": "example.test",
        "procedure_name": "my_method",
        "id": "123abc",
        "result": "value",
        "service_name": "foo",
        "process_name": "bar",
    }


@pytest.mark.asyncio
async def test_send_event(dummy_bus: BusPath, get_dummy_events):
    dummy_bus.client.plugin_registry.manually_set_plugins(
        plugins=[MetricsPlugin(service_name="foo", process_name="bar")]
    )
    dummy_bus.client.register_api(TestApi())
    await dummy_bus.example.test.my_event.fire_async(f=123)

    # What events were fired?
    event_messages = get_dummy_events()
    assert len(event_messages) == 2  # First is the actual event, followed by the metrics event

    # rpc_response_received
    assert event_messages[1].api_name == "internal.metrics"
    assert event_messages[1].event_name == "event_fired"
    assert event_messages[1].kwargs.pop("timestamp")
    assert event_messages[1].kwargs == {
        "api_name": "example.test",
        "event_name": "my_event",
        "event_id": "event_id",
        "kwargs": {"f": 123},
        "service_name": "foo",
        "process_name": "bar",
    }


@pytest.mark.asyncio
async def test_execute_events(dummy_bus: BusPath, dummy_listener, get_dummy_events, mocker):
    event_transport = dummy_bus.client.transport_registry.get_event_transport("default")
    mocker.patch.object(
        event_transport,
        "_get_fake_message",
        return_value=EventMessage(
            api_name="example.test", event_name="my_event", kwargs={"f": 123}
        ),
    )

    await dummy_listener("example.test", "my_event")

    # Setup the bus and do the call
    dummy_bus.client.plugin_registry.manually_set_plugins(
        plugins=[MetricsPlugin(service_name="foo", process_name="bar")]
    )
    dummy_bus.client.register_api(TestApi())

    # The dummy transport will fire an every every 0.1 seconds
    await asyncio.sleep(0.15)

    event_messages = get_dummy_events()
    assert len(event_messages) == 2

    # before_rpc_execution
    assert event_messages[0].api_name == "internal.metrics"
    assert event_messages[0].event_name == "event_received"
    assert event_messages[0].kwargs.pop("timestamp")
    assert event_messages[0].kwargs == {
        "api_name": "example.test",
        "event_name": "my_event",
        "event_id": "event_id",
        "kwargs": {"f": 123},
        "service_name": "foo",
        "process_name": "bar",
    }

    # after_rpc_execution
    assert event_messages[1].api_name == "internal.metrics"
    assert event_messages[1].event_name == "event_processed"
    assert event_messages[1].kwargs.pop("timestamp")
    assert event_messages[1].kwargs == {
        "api_name": "example.test",
        "event_name": "my_event",
        "event_id": "event_id",
        "kwargs": {"f": 123},
        "service_name": "foo",
        "process_name": "bar",
    }
