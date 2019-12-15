import asyncio
import logging
import os
from contextlib import contextmanager, ContextDecorator
from copy import copy
from functools import wraps

from typing import List, Dict, Tuple, TypeVar, Type, NamedTuple, Optional

from lightbus import (
    RpcTransport,
    EventTransport,
    SchemaTransport,
    ResultTransport,
    RpcMessage,
    ResultMessage,
    EventMessage,
)
from lightbus.client.commands import SendEventCommand, CallRpcCommand
from lightbus.path import BusPath
from lightbus.client import BusClient
from lightbus.transports.registry import TransportRegistry

_registry: Dict[str, List] = {}
logger = logging.getLogger(__name__)


class MockResult:
    """Utility for mocking bus calls

    We use camel case method names here for consistency with unittest
    """

    def __init__(self, mocker_context: "BusQueueMockerContext"):
        self.mocker_context = mocker_context

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
        commands = self.mocker_context.event.to_transport.commands.get_all(SendEventCommand)
        if full_event_name is None:
            return [c.message for c in commands]
        else:
            return [c.message for c in commands if c.message.canonical_name == full_event_name]

    get_event_messages = getEventMessages

    def mockEventFiring(self, full_event_name: str):
        # TODO: Argh, how do we do this given that transports can now be
        #       created spontaneously by the pools?
        api_name, _ = full_event_name.rsplit(".", 1)
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
        commands = self.mocker_context.rpc_result.to_transport.commands.get_all(CallRpcCommand)
        if full_rpc_name is None:
            return [c.message for c in commands]
        else:
            return [c.message for c in commands if c.message.canonical_name == full_rpc_name]

    get_rpc_messages = getRpcMessages

    def mockRpcCall(self, full_rpc_name, result=None, **rpc_result_message_kwargs):
        # TODO: Argh, how do we do this given that transports can now be
        #       created spontaneously by the pools?
        self.result.add_mock_response(
            full_rpc_name, dict(result=result, **rpc_result_message_kwargs)
        )

    mock_rpc_call = mockRpcCall

    @property
    def eventNamesFired(self) -> List[str]:
        return [c.canonical_name for c in self.getEventMessages()]

    event_names_fired = eventNamesFired

    @property
    def rpcNamesCalled(self) -> List[str]:
        return [c.canonical_name for c in self.getRpcMessages()]

    rpc_names_called = rpcNamesCalled

    def __repr__(self):
        return f"<MockResult: events: {len(self.getEventMessages())}, rpcs: {len(self.getRpcMessages())}>"


class BusMocker(ContextDecorator):
    def __init__(self, bus: BusPath, require_mocking=True):
        self.bus = bus
        self.old_transport_registry = None
        self.require_mocking = require_mocking
        self.stack: List[BusQueueMockerContext] = []

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
        new_registry = TransportRegistry()
        new_registry.set_schema_transport(
            TestSchemaTransport, TestSchemaTransport.Config(), self.bus.client.config
        )

        self.old_transport_registry = self.bus.client.transport_registry

        for api_name, entry in self.old_transport_registry._registry.items():
            new_registry.set_rpc_transport(
                api_name, TestRpcTransport, TestRpcTransport.Config(), self.bus.client.config
            )
            new_registry.set_result_transport(
                api_name,
                ResultTransport,
                ResultTransport.Config(require_mocking=self.require_mocking),
                self.bus.client.config,
            )
            new_registry.set_event_transport(
                api_name,
                EventTransport,
                EventTransport.Config(require_mocking=self.require_mocking),
                self.bus.client.config,
            )

        self.bus.client.transport_registry = new_registry

        queue_mocker = BusQueueMockerContext(self.bus.client)
        bus_with_mocked_queues = queue_mocker.__enter__()
        self.stack.append(queue_mocker)
        return MockResult(bus_with_mocked_queues)

    def __exit__(self, exc_type, exc, exc_tb):
        """Restores the bus back to its original state"""
        bus_with_mocked_queues = self.stack.pop()
        bus_with_mocked_queues.__exit__()
        bus_with_mocked_queues.bus.client.transport_registry = self.old_transport_registry


bus_mocker = BusMocker


class TestRpcTransport(RpcTransport):
    def __init__(self):
        self.rpcs: List[Tuple[RpcMessage, dict]] = []

    async def call_rpc(self, rpc_message, options: dict):
        pass

    async def consume_rpcs(self, apis):
        raise NotImplementedError("Not yet supported by mocks")


class TestResultTransport(ResultTransport):
    def __init__(self, require_mocking=True):
        super().__init__()
        self.mock_responses = {}
        self.require_mocking = require_mocking

    async def get_return_path(self, rpc_message):
        return "test://"

    async def send_result(self, rpc_message, result_message, return_path):
        raise NotImplementedError("Not yet supported by mocks")

    async def receive_result(self, rpc_message, return_path, options):
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

    async def send_event(self, event_message, options):
        if self.require_mocking:
            assert event_message.canonical_name in self.mock_events, (
                f"Event {event_message.canonical_name} unexpectedly fired. "
                f"Perhaps you need to use mockEventFiring() to ensure the mocker expects this call."
            )

    def add_mock_event(self, full_rpc_name):
        self.mock_events.add(full_rpc_name)

    def consume(self, listen_for: List[Tuple[str, str]], listener_name: str, **kwargs):
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


T = TypeVar("T")


class CommandList(list):
    def types(self):
        return [type(i for i in self)]

    def get_all(self, type_: Type[T]) -> List[T]:
        commands = []
        for i in self:
            if type(i) == type_:
                commands.append(i)
        return commands

    def get(self, type_: Type[T], multiple_ok=False) -> T:
        commands = self.get_all(type_)
        if not commands:
            raise ValueError(f"No command found of type {type_}")
        elif len(commands) > 1 and not multiple_ok:
            raise ValueError(f"Multiple ({len(commands)}) commands found of type {type_}")
        else:
            return commands[0]

    def has(self, type_: Type[T]):
        return any(type(i) == type_ for i in self)

    def count(self, type_: Type[T]):
        len(self.get_all(type_))


class QueueMockContext:
    def __init__(self, queue: asyncio.Queue):
        self.queue = queue
        self.put_items = []
        self.got_items = []
        self._old_get = None
        self._old_put = None

    def __enter__(self) -> "QueueMockContext":
        self._old_put = self.queue.put_nowait
        self.queue.put_nowait = self._put_nowait

        self._old_get = self.queue.get_nowait
        self.queue.get_nowait = self._get_nowait

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def _put_nowait(self, item, *args, **kwargs):
        self.put_items.append(item)
        return self._old_put(item, *args, **kwargs)

    def _get_nowait(self, *args, **kwargs):
        item = self._old_get(*args, **kwargs)
        self.got_items.append(item)
        return item

    @property
    def put_commands(self) -> CommandList:
        return CommandList([i[0] for i in self.put_items])

    @property
    def got_commands(self) -> CommandList:
        return CommandList([i[0] for i in self.got_items])

    @property
    def items(self):
        # Just a little shortcut for the common use case
        return self.put_items

    @property
    def commands(self):
        # Just a little shortcut for the common use case
        return self.put_commands


class BusQueueMockerContext:
    class Queues(NamedTuple):
        to_transport: QueueMockContext
        from_transport: QueueMockContext

    def __init__(self, bus: BusClient):
        self.bus = bus
        self.event: Optional[BusQueueMockerContext.Queues] = None
        self.rpc_result: Optional[BusQueueMockerContext.Queues] = None

    def __enter__(self):
        self.event = BusQueueMockerContext.Queues(
            to_transport=QueueMockContext(self.bus.event_client.producer.queue).__enter__(),
            from_transport=QueueMockContext(self.bus.event_client.consumer.queue).__enter__(),
        )
        self.rpc_result = BusQueueMockerContext.Queues(
            to_transport=QueueMockContext(self.bus.rpc_result_client.producer.queue).__enter__(),
            from_transport=QueueMockContext(self.bus.rpc_result_client.consumer.queue).__enter__(),
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.event.to_transport.__exit__(exc_type, exc_val, exc_tb)
        self.event.from_transport.__exit__(exc_type, exc_val, exc_tb)
        self.rpc_result.to_transport.__exit__(exc_type, exc_val, exc_tb)
        self.rpc_result.from_transport.__exit__(exc_type, exc_val, exc_tb)
