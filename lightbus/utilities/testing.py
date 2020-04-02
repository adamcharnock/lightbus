import asyncio
import logging
from contextlib import ContextDecorator
from functools import wraps
from unittest.mock import patch, _patch

from typing import List, Dict, Tuple, TypeVar, Type, NamedTuple, Optional, Set

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
from lightbus.config import Config
from lightbus.path import BusPath
from lightbus.client import BusClient
from lightbus.transports.registry import TransportRegistry
from lightbus.utilities.internal_queue import InternalQueue

_registry: Dict[str, List] = {}
logger = logging.getLogger(__name__)


class MockResult:
    """Utility for mocking bus calls

    This is the context variable provided when using the BusMocker utility.

    Examples:

        # bus_mock will be a MockResult
        with bus_mocker as bus_mock:
            # ... do stuff with the bus ...
            bus_mock.assertEventFired()

    """

    def __init__(
        self, mocker_context: "BusQueueMockerContext", mock_responses: dict, mock_events: set
    ):
        self.mocker_context = mocker_context
        self.mock_responses = mock_responses
        self.mock_events = mock_events

    def assertEventFired(self, full_event_name, *, times=None):
        event_names_fired = self.eventNamesFired
        if times is None or times > 0:
            assert (  # nosec
                full_event_name in event_names_fired
            ), f"Event {full_event_name} was never fired. Fired events were: {', '.join(event_names_fired)}"

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
        self.mock_events.add(full_event_name)

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
        self.mock_responses[full_rpc_name] = dict(result=result, **rpc_result_message_kwargs)

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

        # Mutable structures which we will use this to get the mocked data into
        # the transports

        # RPC
        mock_responses = {}
        # Events
        mock_events = set()

        # Create our transport classes, into which we inject our mutable structures
        TestRpcTransport = make_test_rpc_transport()
        TestResultTransport = make_test_result_transport(mock_responses)
        TestEventTransport = make_test_event_transport(mock_events)
        TestSchemaTransport = make_test_schema_transport()

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
                TestResultTransport,
                TestResultTransport.Config(require_mocking=self.require_mocking),
                self.bus.client.config,
            )
            new_registry.set_event_transport(
                api_name,
                TestEventTransport,
                TestEventTransport.Config(require_mocking=self.require_mocking),
                self.bus.client.config,
            )

        # The docs are only available on the bus client during testing
        self.bus.client.event_dock.transport_registry = new_registry
        self.bus.client.rpc_result_dock.transport_registry = new_registry

        queue_mocker = BusQueueMockerContext(client=self.bus.client)
        bus_with_mocked_queues = queue_mocker.__enter__()
        self.stack.append(queue_mocker)
        return MockResult(
            bus_with_mocked_queues, mock_responses=mock_responses, mock_events=mock_events
        )

    def __exit__(self, exc_type, exc, exc_tb):
        """Restores the bus back to its original state"""
        bus_with_mocked_queues = self.stack.pop()
        bus_with_mocked_queues.__exit__(exc_type, exc, exc_tb)

        # The docs are only available on the bus client during testing
        bus_with_mocked_queues.client.event_dock.transport_registry = self.old_transport_registry
        bus_with_mocked_queues.client.rpc_result_dock.transport_registry = (
            self.old_transport_registry
        )


bus_mocker = BusMocker

T = TypeVar("T")


def make_test_rpc_transport():
    class TestRpcTransport(RpcTransport):
        def __init__(self):
            self.rpcs: List[Tuple[RpcMessage, dict]] = []

        async def call_rpc(self, rpc_message, options: dict):
            pass

        async def consume_rpcs(self, apis):
            raise NotImplementedError("Not yet supported by mocks")

    return TestRpcTransport


def make_test_result_transport(mock_responses: Dict[str, dict]):
    class TestResultTransport(ResultTransport):
        _mock_responses: Dict[str, dict] = mock_responses

        def __init__(self, require_mocking=True):
            super().__init__()
            self.mock_responses = mock_responses
            self.require_mocking = require_mocking

        @classmethod
        def from_config(cls: Type[T], config: "Config", require_mocking: bool = True) -> T:
            return cls(require_mocking=require_mocking)

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
                kwargs = self.mock_responses[rpc_message.canonical_name].copy()
                kwargs.setdefault("api_name", rpc_message.api_name)
                kwargs.setdefault("procedure_name", rpc_message.procedure_name)
                kwargs.setdefault("rpc_message_id", rpc_message.id)
                return ResultMessage(**kwargs)
            else:
                return ResultMessage(
                    result=None,
                    rpc_message_id="1",
                    api_name=rpc_message.api_name,
                    procedure_name=rpc_message.procedure_name,
                )

    return TestResultTransport


def make_test_event_transport(mock_events: set):
    class TestEventTransport(EventTransport):
        _mock_events: set = mock_events

        def __init__(self, require_mocking=True):
            super().__init__()
            self.events = []
            self.mock_events = mock_events
            self.require_mocking = require_mocking

        @classmethod
        def from_config(cls: Type[T], config: "Config", require_mocking: bool = True) -> T:
            return cls(require_mocking=require_mocking)

        async def send_event(self, event_message, options):
            if self.require_mocking:
                assert event_message.canonical_name in self.mock_events, (
                    f"Event {event_message.canonical_name} unexpectedly fired. "
                    f"Perhaps you need to use mockEventFiring() to ensure the mocker expects this call."
                )

        async def consume(self, listen_for: List[Tuple[str, str]], listener_name: str, **kwargs):
            """Consume RPC events for the given API"""
            raise NotImplementedError("Not yet supported by mocks")

    return TestEventTransport


def make_test_schema_transport():
    class TestSchemaTransport(SchemaTransport):
        def __init__(self):
            self.schemas = {}

        async def store(self, api_name: str, schema: Dict, ttl_seconds: int):
            self.schemas[api_name] = schema

        async def ping(self, api_name: str, schema: Dict, ttl_seconds: int):
            pass

        async def load(self) -> Dict[str, Dict]:
            return self.schemas

    return TestSchemaTransport


# Command mocking
# These tools are not part of the public API, but are used for internal testing,
# and to power the above BusMocker (which is part of the public API)


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


Command = TypeVar("Command", bound=NamedTuple)


class QueueMockContext:
    def __init__(self, queue: InternalQueue):
        self.queue = queue
        self.put_items: List[Tuple[Command, asyncio.Event]] = []
        self.got_items: List[Tuple[Command, asyncio.Event]] = []

        self._patched_put_nowait: Optional[_patch] = None
        self._patched_get_nowait: Optional[_patch] = None

        self._blackhole: Set[Type[Command]] = set()

    def __enter__(self) -> "QueueMockContext":
        self.put_items = []
        self.got_items = []

        self._orig_put_nowait = self.queue.put_nowait
        self._orig_get_nowait = self.queue.get_nowait

        self._patched_put_nowait = patch.object(self.queue, "put_nowait", wraps=self._put_nowait)
        self._patched_get_nowait = patch.object(self.queue, "get_nowait", wraps=self._get_nowait)

        self._patched_put_nowait.start()
        self._patched_get_nowait.start()

        return self

    def blackhole(self, *command_types):
        self._blackhole = self._blackhole.union(set(command_types))

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._patched_put_nowait.stop()
        self._patched_get_nowait.stop()

    def _put_nowait(self, item, *args, **kwargs):
        self.put_items.append(item)

        try:
            command, event = item
        except ValueError:
            # The item being added isn't a command
            pass
        else:
            # Don't call blackholed commands
            if type(command) in self._blackhole:
                return

        return self._orig_put_nowait(item, *args, **kwargs)

    def _get_nowait(self, *args, **kwargs):
        item = self._orig_get_nowait(*args, **kwargs)
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

    def __init__(self, client: BusClient):
        self.client = client
        self.event: Optional[BusQueueMockerContext.Queues] = None
        self.rpc_result: Optional[BusQueueMockerContext.Queues] = None
        self.errors: Optional[QueueMockContext] = None

    def __enter__(self):
        self.event = BusQueueMockerContext.Queues(
            to_transport=QueueMockContext(self.client.event_client.producer.queue).__enter__(),
            from_transport=QueueMockContext(self.client.event_client.consumer.queue).__enter__(),
        )
        self.rpc_result = BusQueueMockerContext.Queues(
            to_transport=QueueMockContext(self.client.rpc_result_client.producer.queue).__enter__(),
            from_transport=QueueMockContext(
                self.client.rpc_result_client.consumer.queue
            ).__enter__(),
        )
        self.errors = QueueMockContext(self.client.error_queue).__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.event.to_transport.__exit__(exc_type, exc_val, exc_tb)
        self.event.from_transport.__exit__(exc_type, exc_val, exc_tb)
        self.rpc_result.to_transport.__exit__(exc_type, exc_val, exc_tb)
        self.rpc_result.from_transport.__exit__(exc_type, exc_val, exc_tb)
