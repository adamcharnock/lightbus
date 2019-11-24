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
)
from lightbus.path import BusPath
from lightbus.client import BusClient
from lightbus.transports.base import TransportRegistry

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
        assert (  # nosec
            full_event_name in event_names_fired
        ), f"Event {full_event_name} was never fired. Fired events were: {set(event_names_fired)}"

        if times:
            total_times_fired = len([v for v in event_names_fired if v == full_event_name])
            assert total_times_fired == times, (  # nosec
                f"Event fired the incorrect number of times. "
                f"Expected {times}, actual {total_times_fired}"
            )

    def assertEventNotFired(self, full_event_name):
        assert (
            full_event_name not in self.eventNamesFired
        ), f"Event {full_event_name} was unexpectedly fired"

    def getEventMessages(self, full_event_name=None):
        if full_event_name is None:
            return [m for m, _ in self.event.events]
        else:
            return [m for m, _ in self.event.events if m.canonical_name == full_event_name]

    def assertRpcCalled(self, full_rpc_name, *, times=None):
        rpc_names_fired = self.rpcNamesFired
        assert (
            full_rpc_name in rpc_names_fired
        ), f"RPC {full_rpc_name} was never called. Called RPCs were: {set(rpc_names_fired)}"

        if times:
            total_times_called = len([v for v in rpc_names_fired if v == full_rpc_name])
            assert total_times_called == times, (  # nosec
                f"RPC {full_rpc_name} called the incorrect number of times. "
                f"Expected {times}, actual {total_times_called}"
            )

    def assertRpcNotCalled(self, full_rpc_name):
        assert (
            full_rpc_name not in self.rpcNamesFired
        ), f"Event {full_rpc_name} was unexpectedly fired"

    def mockRpcCall(self, full_rpc_name, result=None, **rpc_result_message_kwargs):
        self.result.add_mock_response(
            full_rpc_name, dict(result=result, **rpc_result_message_kwargs)
        )

    @property
    def eventNamesFired(self):
        return [em.canonical_name for em, options in self.event.events]

    @property
    def rpcNamesFired(self):
        return [rm.canonical_name for rm, options in self.rpc.rpcs]

    def __repr__(self):
        return f"<MockResult: events: {len(self.event.events)}, rpcs: {len(self.rpc.rpcs)}>"


class BusMocker(ContextDecorator):
    def __init__(self, bus: BusPath):
        self.bus = bus
        self.old_transport_registry = None

    # Overriding ContextDecorator to pass the mock result to the function
    def __call__(self, func):
        @wraps(func)
        def inner(*args, **kwds):
            with self._recreate_cm() as mock_result:
                return func(*args, mock_result, **kwds)

        return inner

    def __enter__(self):
        rpc = TestRpcTransport()
        result = TestResultTransport()
        event = TestEventTransport()
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
    def __init__(self):
        self.mock_responses = {}

    def get_return_path(self, rpc_message):
        return "test://"

    async def send_result(self, rpc_message, result_message, return_path, bus_client: "BusClient"):
        raise NotImplementedError("Not yet supported by mocks")

    async def receive_result(self, rpc_message, return_path, options, bus_client: "BusClient"):
        assert rpc_message.canonical_name in self.mock_responses, (
            f"RPC {rpc_message.canonical_name} unexpectedly called. "
            f"Perhaps you need to use mockRpcCall() to ensure the mocker expects this call."
        )
        kwargs = self.mock_responses[rpc_message.canonical_name]
        return ResultMessage(**kwargs)

    def add_mock_response(self, full_rpc_name, rpc_result_message_kwargs):
        rpc_result_message_kwargs.setdefault("rpc_message_id", 1)
        self.mock_responses[full_rpc_name] = rpc_result_message_kwargs


class TestEventTransport(EventTransport):
    def __init__(self):
        self.events = []
        super().__init__()

    async def send_event(self, event_message, options, bus_client: "BusClient"):
        self.events.append((event_message, options))

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
