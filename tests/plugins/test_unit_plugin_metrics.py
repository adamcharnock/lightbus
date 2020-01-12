import pytest
from typing import Type

from lightbus.api import Api, Event
from lightbus.client.commands import SendEventCommand
from lightbus.client.utilities import OnError
from lightbus.path import BusPath
from lightbus.message import RpcMessage, EventMessage
from lightbus.plugins.metrics import MetricsPlugin
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
async def test_rpc_call(
    dummy_bus: BusPath, worker: Worker, queue_mocker: Type[BusQueueMockerContext]
):
    # Setup the bus and do the call
    dummy_bus.client.plugin_registry.set_plugins(
        plugins=[MetricsPlugin(service_name="foo", process_name="bar")]
    )
    dummy_bus.client.register_api(TestApi())

    async with worker(dummy_bus):
        with queue_mocker(dummy_bus.client) as q:
            await dummy_bus.my_company.auth.check_password.call_async(username="a", password="b")

    event_commands = q.event.to_transport.commands.get_all(SendEventCommand)

    assert len(event_commands) == 4, event_commands

    # Arrange messages in a dict indexed by name (makes checking results)
    event_messages = {}
    for event_command in q.event.to_transport.commands.get_all(SendEventCommand):
        event_messages[event_command.message.event_name] = event_command.message

    # rpc_call_sent
    assert event_messages["rpc_call_sent"].api_name == "internal.metrics"
    assert event_messages["rpc_call_sent"].event_name == "rpc_call_sent"
    assert event_messages["rpc_call_sent"].kwargs["timestamp"]
    assert event_messages["rpc_call_sent"].kwargs["api_name"] == "my_company.auth"
    assert event_messages["rpc_call_sent"].kwargs["procedure_name"] == "check_password"
    assert event_messages["rpc_call_sent"].kwargs["id"]
    assert event_messages["rpc_call_sent"].kwargs["kwargs"] == {"username": "a", "password": "b"}
    assert event_messages["rpc_call_sent"].kwargs["service_name"] == "foo"
    assert event_messages["rpc_call_sent"].kwargs["process_name"] == "bar"

    # rpc_call_received
    assert event_messages["rpc_call_received"].api_name == "internal.metrics"
    assert event_messages["rpc_call_received"].event_name == "rpc_call_received"
    assert event_messages["rpc_call_received"].kwargs["timestamp"]
    assert event_messages["rpc_call_received"].kwargs["api_name"] == "my_company.auth"
    assert event_messages["rpc_call_received"].kwargs["procedure_name"] == "check_password"
    assert event_messages["rpc_call_received"].kwargs["id"]
    assert event_messages["rpc_call_received"].kwargs["service_name"] == "foo"
    assert event_messages["rpc_call_received"].kwargs["process_name"] == "bar"

    # rpc_response_sent
    assert event_messages["rpc_response_sent"].api_name == "internal.metrics"
    assert event_messages["rpc_response_sent"].event_name == "rpc_response_sent"
    assert event_messages["rpc_response_sent"].kwargs["timestamp"]
    assert event_messages["rpc_response_sent"].kwargs["api_name"] == "my_company.auth"
    assert event_messages["rpc_response_sent"].kwargs["procedure_name"] == "check_password"
    assert event_messages["rpc_response_sent"].kwargs["id"]
    assert event_messages["rpc_response_sent"].kwargs["service_name"] == "foo"
    assert event_messages["rpc_response_sent"].kwargs["process_name"] == "bar"
    assert event_messages["rpc_response_sent"].kwargs["result"] == True

    # rpc_response_received
    assert event_messages["rpc_response_received"].api_name == "internal.metrics"
    assert event_messages["rpc_response_received"].event_name == "rpc_response_received"
    assert event_messages["rpc_response_received"].kwargs["timestamp"]
    assert event_messages["rpc_response_received"].kwargs["api_name"] == "my_company.auth"
    assert event_messages["rpc_response_received"].kwargs["procedure_name"] == "check_password"
    assert event_messages["rpc_response_received"].kwargs["id"]
    assert event_messages["rpc_response_received"].kwargs["service_name"] == "foo"
    assert event_messages["rpc_response_received"].kwargs["process_name"] == "bar"


@pytest.mark.asyncio
async def test_send_event(dummy_bus: BusPath, queue_mocker: Type[BusQueueMockerContext]):
    dummy_bus.client.plugin_registry.set_plugins(
        plugins=[MetricsPlugin(service_name="foo", process_name="bar")]
    )
    dummy_bus.client.register_api(TestApi())

    with queue_mocker(dummy_bus.client) as q:
        await dummy_bus.my_company.auth.my_event.fire_async(f=123)

    event_commands = q.event.to_transport.commands.get_all(SendEventCommand)

    # First is the actual event, followed by the metrics event
    assert len(event_commands) == 2, event_commands

    # rpc_response_received
    assert event_commands[1].message.api_name == "internal.metrics"
    assert event_commands[1].message.event_name == "event_fired"
    assert event_commands[1].message.kwargs["timestamp"]
    assert event_commands[1].message.kwargs["api_name"] == "my_company.auth"
    assert event_commands[1].message.kwargs["event_name"] == "my_event"
    assert event_commands[1].message.kwargs["event_id"] == "event_id"
    assert event_commands[1].message.kwargs["kwargs"] == {"f": 123}
    assert event_commands[1].message.kwargs["service_name"] == "foo"
    assert event_commands[1].message.kwargs["process_name"] == "bar"


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
                event_message=event_message,
                listener=lambda *a, **kw: None,
                options={},
                on_error=OnError.SHUTDOWN,
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
