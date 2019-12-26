import asyncio
import pytest
from typing import Type

from lightbus import BusClient
from lightbus.api import Api, Event
from lightbus.client.commands import SendEventCommand
from lightbus.path import BusPath
from lightbus.message import RpcMessage, EventMessage
from lightbus.plugins.metrics import MetricsPlugin
from lightbus.utilities.async_tools import cancel
from lightbus.utilities.testing import BusQueueMockerContext
from tests.conftest import Worker

pytestmark = pytest.mark.unit


class TestApi(Api):
    my_event = Event(parameters=["f"])

    class Meta:
        name = "my_company.auth"

    def check_password(self, username, password):
        return True


@pytest.mark.asyncio
async def test_remote_rpc_call(dummy_bus: BusPath, get_dummy_events):
    # Setup the bus and do the call
    dummy_bus.client.plugin_registry.set_plugins(
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
    # Setup the bus and do the call
    dummy_bus.client.plugin_registry.set_plugins(
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
    assert event_messages[0].kwargs["api_name"] == "my_company.auth"
    assert event_messages[0].kwargs["procedure_name"] == "check_password"
    assert event_messages[0].kwargs["id"]
    assert event_messages[0].kwargs["service_name"] == "foo"
    assert event_messages[0].kwargs["process_name"] == "bar"

    # after_rpc_execution
    assert event_messages[1].api_name == "internal.metrics"
    assert event_messages[1].event_name == "rpc_response_sent"
    assert event_messages[1].kwargs.pop("timestamp")
    assert event_messages[1].kwargs["api_name"] == "my_company.auth"
    assert event_messages[1].kwargs["procedure_name"] == "check_password"
    assert event_messages[1].kwargs["id"]
    assert event_messages[1].kwargs["service_name"] == "foo"
    assert event_messages[1].kwargs["process_name"] == "bar"
    assert event_messages[1].kwargs["result"] == True


@pytest.mark.asyncio
async def test_send_event(dummy_bus: BusPath, get_dummy_events):
    dummy_bus.client.plugin_registry.set_plugins(
        plugins=[MetricsPlugin(service_name="foo", process_name="bar")]
    )
    dummy_bus.client.register_api(TestApi())
    await dummy_bus.my_company.auth.my_event.fire_async(f=123)

    # What events were fired?
    event_messages = get_dummy_events()
    assert len(event_messages) == 2  # First is the actual event, followed by the metrics event

    # rpc_response_received
    assert event_messages[1].api_name == "internal.metrics"
    assert event_messages[1].event_name == "event_fired"
    assert event_messages[1].kwargs.pop("timestamp")
    assert event_messages[1].kwargs == {
        "api_name": "my_company.auth",
        "event_name": "my_event",
        "event_id": "event_id",
        "kwargs": {"f": 123},
        "service_name": "foo",
        "process_name": "bar",
    }


@pytest.mark.asyncio
async def test_execute_events(
    dummy_bus: BusPath, worker: Worker, queue_mocker: Type[BusQueueMockerContext]
):
    dummy_bus.client.register_api(TestApi())
    dummy_bus.client.plugin_registry.set_plugins(
        plugins=[MetricsPlugin(service_name="foo", process_name="bar")]
    )

    event_message = EventMessage(api_name="example.test", event_name="my_event", kwargs={"f": 123})

    async with worker(dummy_bus):
        with queue_mocker(dummy_bus.client) as q:
            await dummy_bus.client.event_client._on_message(
                event_message=event_message, listener=lambda *a, **kw: None, options={}
            )

    # Arrange messages in a dict indexed by name (makes checking results)
    event_messages = {}
    event_message: EventMessage

    for event_command in q.event.to_transport.commands.get_all(SendEventCommand):
        event_messages[event_command.message.canonical_name] = event_command.message

    assert "internal.metrics.event_received" in event_messages
    assert "internal.metrics.event_processed" in event_messages

    # before_rpc_execution
    event = event_messages["internal.metrics.event_received"]
    assert event.api_name == "internal.metrics"
    assert event.event_name == "event_received"
    assert event.kwargs.pop("timestamp")
    assert event.kwargs == {
        "api_name": "example.test",
        "event_name": "my_event",
        "event_id": "event_id",
        "kwargs": {"f": 123},
        "service_name": "foo",
        "process_name": "bar",
    }

    # after_rpc_execution
    event = event_messages["internal.metrics.event_processed"]
    assert event.api_name == "internal.metrics"
    assert event.event_name == "event_processed"
    assert event.kwargs.pop("timestamp")
    assert event.kwargs == {
        "api_name": "example.test",
        "event_name": "my_event",
        "event_id": "event_id",
        "kwargs": {"f": 123},
        "service_name": "foo",
        "process_name": "bar",
    }
