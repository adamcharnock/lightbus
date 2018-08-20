import logging
import os
from contextlib import contextmanager, ContextDecorator
from copy import copy
from functools import wraps
from unittest.mock import patch

from typing import List, Dict, Tuple

from lightbus import RpcTransport, EventTransport, SchemaTransport, ResultTransport, EventMessage
from lightbus.path import BusPath
from lightbus.transports.base import TransportRegistry

_registry: Dict[str, List] = {}
logger = logging.getLogger(__name__)

# Work in progress for testing crashes


def test_hook(hook_name: str):
    for callback in _registry.get(hook_name, []):
        logger.warning(f"Executing test hook {hook_name} on hook {callback}")
        callback()


@contextmanager
def register_callback(hook_name, callback):
    _registry.setdefault(hook_name, [])
    _registry[hook_name].append(callback)
    yield
    _registry[hook_name].remove(callback)


@contextmanager
def crash_at(hook_name):
    return register_callback(hook_name, os._exit)


# Utility for mocking bus calls


class MockResult(object):

    def __init__(self, rpc_transport, result_transport, event_transport, schema_transport):
        self.rpc: TestRpcTransport = rpc_transport
        self.result: TestResultTransport = result_transport
        self.event: TestEventTransport = event_transport
        self.schema: TestSchemaTransport = schema_transport

    def assertEventFired(self, full_event_name, *, times=None):
        fired_events = [em.canonical_name for em, options in self.event.events]
        assert (
            full_event_name in fired_events
        ), f"Event {full_event_name} was never fired. Fired events were: {set(fired_events)}"

        if times:
            total_times_fired = len([v for v in fired_events if v == full_event_name])
            assert (
                total_times_fired == times
            ), f"Event fired the incorrect number of times. " f"Expected {times}, actual {total_times_fired}"

    def getEventMessages(self, full_event_name=None):
        if full_event_name is None:
            return [m for m, _ in self.event.events]
        else:
            return [m for m, _ in self.event.events if m.canonical_name == full_event_name]

    def __repr__(self):
        return f"<MockResult: events: {len(self.event.events)}>"


class BusMocker(ContextDecorator):

    def __init__(self, bus: BusPath):
        self.bus = bus

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
        self.rpcs = []

    async def call_rpc(self, rpc_message, options: dict):
        self.rpcs.append((rpc_message, options))

    async def consume_rpcs(self, apis):
        raise NotImplementedError("Not yet supported by mocks")


class TestResultTransport(ResultTransport):

    def get_return_path(self, rpc_message):
        return "test://"

    async def send_result(self, rpc_message, result_message, return_path):
        raise NotImplementedError("Not yet supported by mocks")

    async def receive_result(self, rpc_message, return_path, options):
        raise NotImplementedError("Not yet supported by mocks")


class TestEventTransport(EventTransport):

    def __init__(self):
        self.events = []

    async def send_event(self, event_message, options):
        self.events.append((event_message, options))

    def consume(self, listen_for: List[Tuple[str, str]], consumer_group: str = None, **kwargs):
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
