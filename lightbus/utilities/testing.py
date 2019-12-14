import logging
import os
from contextlib import contextmanager, ContextDecorator
from copy import copy
from functools import wraps

from typing import List, Dict, Tuple

from lightbus import (
    RpcTransport,
    EventTransport,
    SchemaTransport,
    ResultTransport,
    RpcMessage,
    ResultMessage,
    EventMessage,
)
from lightbus.path import BusPath
from lightbus.client import BusClient
from lightbus.transports.registry import TransportRegistry

_registry: Dict[str, List] = {}
logger = logging.getLogger(__name__)


class MockResult:
    """Utility for mocking bus calls

    We use camel case method names here for consistency with unittest
    """

    def __init__(self, rpc_transport, result_transport, event_transport, schema_transport):
        self.rpc: TestRpcTransport = rpc_transport
        self.result: TestResultTransport = result_transport
        self.event: TestEventTransport = event_transport
        self.schema: TestSchemaTransport = schema_transport

    def assertEventFired(self, full_event_name, *, times=None):
        event_names_fired = self.eventNamesFired
        if times is None or times > 0:
            assert (  # nosec
                full_event_name in event_names_fired
            ), f"Event {full_event_name} was never fired. Fired events were: {set(event_names_fired)}"

        if times is not None:
            total_times_fired = len([v for v in event_names_fired if v == full_event_name])
            assert total_times_fired == times, (  # nosec
                f"Event fired the incorrect number of times. "
                f"Expected {times}, actual {total_times_fired}"
            )

    assert_events_fired = assertEventFired

    def assertEventNotFired(self, full_event_name):
        assert (
            full_event_name not in self.eventNamesFired
        ), f"Event {full_event_name} was unexpectedly fired"

    assert_event_not_fired = assertEventNotFired

    def getEventMessages(self, full_event_name=None) -> List[EventMessage]:
        if full_event_name is None:
            return [m for m, _ in self.event.events]
        else:
            return [m for m, _ in self.event.events if m.canonical_name == full_event_name]

    get_event_messages = getEventMessages

    def mockEventFiring(self, full_event_name):
        self.event.add_mock_event(full_event_name)

    mock_event_firing = mockEventFiring

    def assertRpcCalled(self, full_rpc_name, *, times=None):
        rpc_names_called = self.rpcNamesCalled
        if times is None or times > 0:
            assert (
                full_rpc_name in rpc_names_called
            ), f"RPC {full_rpc_name} was never called. Called RPCs were: {set(rpc_names_called)}"

        if times is not None:
            total_times_called = len([v for v in rpc_names_called if v == full_rpc_name])
            assert total_times_called == times, (  # nosec
                f"RPC {full_rpc_name} called the incorrect number of times. "
                f"Expected {times}, actual {total_times_called}"
            )

    assert_rpc_called = assertRpcCalled

    def assertRpcNotCalled(self, full_rpc_name):
        assert (
            full_rpc_name not in self.rpcNamesCalled
        ), f"Event {full_rpc_name} was unexpectedly fired"

    assert_rpc_not_called = assertRpcNotCalled

    def getRpcMessages(self, full_rpc_name=None) -> List[RpcMessage]:
        if full_rpc_name is None:
            return [m for m, _ in self.rpc.rpcs]
        else:
            return [m for m, _ in self.rpc.rpcs if m.canonical_name == full_rpc_name]

    get_rpc_messages = getRpcMessages

    def mockRpcCall(self, full_rpc_name, result=None, **rpc_result_message_kwargs):
        self.result.add_mock_response(
            full_rpc_name, dict(result=result, **rpc_result_message_kwargs)
        )

    mock_rpc_call = mockRpcCall

    @property
    def eventNamesFired(self) -> List[str]:
        return [em.canonical_name for em, options in self.event.events]

    event_names_fired = eventNamesFired

    @property
    def rpcNamesCalled(self) -> List[str]:
        return [rm.canonical_name for rm, options in self.rpc.rpcs]

    rpc_names_called = rpcNamesCalled

    def __repr__(self):
        return f"<MockResult: events: {len(self.event.events)}, rpcs: {len(self.rpc.rpcs)}>"


class BusMocker(ContextDecorator):
    def __init__(self, bus: BusPath, require_mocking=True):
        self.bus = bus
        self.old_transport_registry = None
        self.require_mocking = require_mocking

    def __call__(self, func):
        # Overriding ContextDecorator.__call__ to pass the mock
        # result to the function. This is exactly the same as the parent
        # implementation except we pass mock_result into func().
        @wraps(func)
        def inner(*args, **kwds):
            with self._recreate_cm() as mock_result:
                return func(*args, mock_result, **kwds)

        return inner

    def __enter__(self):
        """Start of a context where all the bus' transports have been replaced with mocks"""
        rpc = TestRpcTransport()
        result = TestResultTransport(require_mocking=self.require_mocking)
        event = TestEventTransport(require_mocking=self.require_mocking)
        schema = TestSchemaTransport()

        new_registry = TransportRegistry()
        new_registry.set_schema_transport(schema)

        self.old_transport_registry = self.bus.client.transport_registry

        for api_name, entry in self.old_transport_registry._registry.items():
            new_registry.set_rpc_transport(api_name, rpc)
            new_registry.set_result_transport(api_name, result)

            if hasattr(entry.event, "child_transport"):
                parent_transport = copy(entry.event)
                parent_transport.child_transport = event
                new_registry.set_event_transport(api_name, parent_transport)
            else:
                new_registry.set_event_transport(api_name, event)

        self.bus.client.transport_registry = new_registry
        return MockResult(rpc, result, event, schema)

    def __exit__(self, exc_type, exc, exc_tb):
        """Restores the bus back to its original state"""
        self.bus.client.transport_registry = self.old_transport_registry


bus_mocker = BusMocker


class TestRpcTransport(RpcTransport):
    def __init__(self):
        self.rpcs: List[Tuple[RpcMessage, dict]] = []

    async def call_rpc(self, rpc_message, options: dict, bus_client: "BusClient"):
        self.rpcs.append((rpc_message, options))

    async def consume_rpcs(self, apis, bus_client: "BusClient"):
        raise NotImplementedError("Not yet supported by mocks")


class TestResultTransport(ResultTransport):
    def __init__(self, require_mocking=True):
        super().__init__()
        self.mock_responses = {}
        self.require_mocking = require_mocking

    def get_return_path(self, rpc_message):
        return "test://"

    async def send_result(self, rpc_message, result_message, return_path, bus_client: "BusClient"):
        raise NotImplementedError("Not yet supported by mocks")

    async def receive_result(self, rpc_message, return_path, options, bus_client: "BusClient"):
        if self.require_mocking:
            assert rpc_message.canonical_name in self.mock_responses, (
                f"RPC {rpc_message.canonical_name} unexpectedly called. "
                f"Perhaps you need to use mockRpcCall() to ensure the mocker expects this call."
            )

        if rpc_message.canonical_name in self.mock_responses:
            kwargs = self.mock_responses[rpc_message.canonical_name]
            return ResultMessage(**kwargs)
        else:
            return ResultMessage(result=None, rpc_message_id="1")

    def add_mock_response(self, full_rpc_name, rpc_result_message_kwargs):
        rpc_result_message_kwargs.setdefault("rpc_message_id", 1)
        self.mock_responses[full_rpc_name] = rpc_result_message_kwargs


class TestEventTransport(EventTransport):
    def __init__(self, require_mocking=True):
        super().__init__()
        self.events = []
        self.mock_events = set()
        self.require_mocking = require_mocking

    async def send_event(self, event_message, options, bus_client: "BusClient"):
        if self.require_mocking:
            assert event_message.canonical_name in self.mock_events, (
                f"Event {event_message.canonical_name} unexpectedly fired. "
                f"Perhaps you need to use mockEventFiring() to ensure the mocker expects this call."
            )
        self.events.append((event_message, options))

    def add_mock_event(self, full_rpc_name):
        self.mock_events.add(full_rpc_name)

    def consume(
        self,
        listen_for: List[Tuple[str, str]],
        listener_name: str,
        bus_client: "BusClient",
        **kwargs,
    ):
        """Consume RPC events for the given API"""
        raise NotImplementedError("Not yet supported by mocks")


class TestSchemaTransport(SchemaTransport):
    def __init__(self):
        self.schemas = {}

    async def store(self, api_name: str, schema: Dict, ttl_seconds: int):
        self.schemas[api_name] = schema

    async def ping(self, api_name: str, schema: Dict, ttl_seconds: int):
        pass

    async def load(self) -> Dict[str, Dict]:
        return self.schemas
