import asyncio
import threading
from unittest.mock import MagicMock

import janus
import pytest
from typing import Type

import lightbus
import lightbus.creation
import lightbus.path
from lightbus import EventMessage
from lightbus.client.commands import SendResultCommand, ConsumeEventsCommand
from lightbus.client.utilities import Error
from lightbus.config import Config
from lightbus.exceptions import (
    UnknownApi,
    EventNotFound,
    InvalidEventArguments,
    InvalidEventListener,
    TransportNotFound,
    InvalidName,
)
from lightbus.transports.registry import TransportRegistry
from lightbus.utilities.testing import BusQueueMockerContext
from tests.conftest import Worker

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_fire_event_api_doesnt_exist(dummy_bus: lightbus.path.BusPath):
    with pytest.raises(UnknownApi):
        await dummy_bus.client.fire_event("non_existent_api", "event")


@pytest.mark.asyncio
async def test_fire_event_event_doesnt_exist(dummy_bus: lightbus.path.BusPath, dummy_api):
    dummy_bus.client.register_api(dummy_api)
    with pytest.raises(EventNotFound):
        await dummy_bus.client.fire_event("my.dummy", "bad_event")


@pytest.mark.asyncio
async def test_fire_event_bad_event_arguments(dummy_bus: lightbus.path.BusPath, dummy_api):
    dummy_bus.client.register_api(dummy_api)
    with pytest.raises(InvalidEventArguments):
        await dummy_bus.client.fire_event("my.dummy", "my_event", kwargs={"bad_arg": "value"})


@pytest.mark.asyncio
async def test_listen_for_event_non_callable(dummy_bus: lightbus.path.BusPath):
    with pytest.raises(InvalidEventListener):
        dummy_bus.client.listen_for_event(
            "my.dummy", "my_event", listener=123, listener_name="test"
        )


@pytest.mark.asyncio
async def test_listen_for_event_no_args(dummy_bus: lightbus.path.BusPath):
    with pytest.raises(InvalidEventListener):
        dummy_bus.client.listen_for_event(
            "my.dummy", "my_event", listener=lambda: None, listener_name="test"
        )


@pytest.mark.asyncio
async def test_listen_for_event_no_positional_args(dummy_bus: lightbus.path.BusPath):
    with pytest.raises(InvalidEventListener):
        dummy_bus.client.listen_for_event(
            "my.dummy", "my_event", listener=lambda **kw: None, listener_name="test"
        )


@pytest.mark.asyncio
async def test_listen_for_event_one_positional_arg(dummy_bus: lightbus.path.BusPath):
    dummy_bus.client.listen_for_event(
        "my.dummy", "my_event", listener=lambda event_message: None, listener_name="test"
    )


@pytest.mark.asyncio
async def test_listen_for_event_two_positional_args(dummy_bus: lightbus.path.BusPath):
    dummy_bus.client.listen_for_event(
        "my.dummy", "my_event", listener=lambda event_message, other: None, listener_name="test"
    )


@pytest.mark.asyncio
async def test_listen_for_event_variable_positional_args(dummy_bus: lightbus.path.BusPath):
    dummy_bus.client.listen_for_event(
        "my.dummy", "my_event", listener=lambda *a: None, listener_name="test"
    )


@pytest.mark.asyncio
async def test_listen_for_event_starts_with_underscore(dummy_bus: lightbus.path.BusPath):
    with pytest.raises(InvalidName):
        dummy_bus.client.listen_for_event(
            "my.dummy", "_my_event", listener=lambda *a, **kw: None, listener_name="test"
        )


@pytest.mark.asyncio
async def test_fire_event_starts_with_underscore(dummy_bus: lightbus.path.BusPath, dummy_api):
    dummy_bus.client.register_api(dummy_api)
    with pytest.raises(InvalidName):
        await dummy_bus.client.fire_event("my.dummy", "_my_event")


@pytest.mark.asyncio
async def test_call_rpc_remote_starts_with_underscore(dummy_bus: lightbus.path.BusPath):
    with pytest.raises(InvalidName):
        await dummy_bus.client.call_rpc_remote("my.dummy", "_my_event")


@pytest.mark.asyncio
async def test_call_rpc_local_starts_with_underscore(dummy_bus: lightbus.path.BusPath, dummy_api):
    dummy_bus.client.register_api(dummy_api)
    with pytest.raises(InvalidName):
        await dummy_bus.client.rpc_result_client._call_rpc_local("my.dummy", "_my_event")


@pytest.mark.asyncio
async def test_consume_rpcs_with_transport_error(
    mocker, dummy_bus: lightbus.path.BusPath, dummy_api, queue_mocker: Type[BusQueueMockerContext]
):
    class TestException(Exception):
        pass

    async def co(*a, **kw):
        raise TestException()

    dummy_bus.client.register_api(dummy_api)

    mocker.patch.object(dummy_bus.client.rpc_result_client, "_call_rpc_local", side_effect=co)
    fut = asyncio.Future()
    fut.set_result(None)

    await dummy_bus.client.lazy_load_now()

    with queue_mocker(dummy_bus.client) as q:
        await dummy_bus.client.consume_rpcs(apis=[dummy_api])

        # Wait for the consumer to send the result
        for _ in range(0, 20):
            if q.rpc_result.to_transport.commands.has(SendResultCommand):
                break
            await asyncio.sleep(0.05)

    send_result_command = q.rpc_result.to_transport.commands.get(SendResultCommand)
    result_message = send_result_command.message

    assert result_message.error
    assert result_message.result
    assert result_message.trace


@pytest.mark.asyncio
async def test_listen_for_event_empty_name(dummy_bus: lightbus.path.BusPath):
    with pytest.raises(InvalidName):
        dummy_bus.client.listen_for_event(
            "my.dummy", "_my_event", listener=lambda *a: None, listener_name="test"
        )


@pytest.mark.asyncio
async def test_fire_event_empty_name(dummy_bus: lightbus.path.BusPath, dummy_api):
    dummy_bus.client.register_api(dummy_api)
    with pytest.raises(InvalidName):
        await dummy_bus.client.fire_event("my.dummy", "_my_event")


@pytest.mark.asyncio
async def test_fire_event_version(dummy_bus: lightbus.path.BusPath, mocker):
    class ApiWithVersion(lightbus.Api):
        my_event = lightbus.Event()

        class Meta:
            name = "versioned_api"
            version = 5

    dummy_bus.client.register_api(ApiWithVersion())

    send_event_spy = mocker.spy(
        dummy_bus.client.transport_registry.get_event_transport("versioned_api"), "send_event"
    )

    await dummy_bus.client.fire_event("versioned_api", "my_event")
    assert send_event_spy.called
    (message,), _ = send_event_spy.call_args
    assert message.version == 5


@pytest.mark.asyncio
async def test_call_rpc_remote_empty_name(dummy_bus: lightbus.path.BusPath):
    with pytest.raises(InvalidName):
        await dummy_bus.client.call_rpc_remote("my.dummy", "_my_event")


@pytest.mark.asyncio
async def test_call_rpc_local_empty_name(dummy_bus: lightbus.path.BusPath, dummy_api):
    dummy_bus.client.register_api(dummy_api)
    with pytest.raises(InvalidName):
        await dummy_bus.client.rpc_result_client._call_rpc_local("my.dummy", "_my_event")


@pytest.mark.asyncio
async def test_no_transport(dummy_bus):
    # No transports configured for any relevant api
    dummy_bus.client.transport_registry._registry = {}
    with pytest.raises(TransportNotFound):
        await dummy_bus.client.call_rpc_remote("my_api", "test", kwargs={}, options={})


@pytest.mark.asyncio
async def test_no_transport_type(dummy_bus):
    # Transports configured, but the wrong type of transport
    # No transports configured for any relevant api
    registry_entry = dummy_bus.client.transport_registry._registry["default"]
    dummy_bus.client.transport_registry._registry = {"default": registry_entry._replace(rpc=None)}
    with pytest.raises(TransportNotFound):
        await dummy_bus.client.call_rpc_remote("my_api", "test", kwargs={}, options={})


def test_setup_transports_opened(mocker):
    rpc_transport = lightbus.DebugRpcTransport()

    # TODO: There has to be a cleaner way of patching a coroutine.
    #       Do a global search for 'dummy_coroutine' if you find it
    async def dummy_coroutine(*args, **kwargs):
        pass

    m = mocker.patch.object(rpc_transport, "open", autospec=True, return_value=dummy_coroutine())

    transport_registry = TransportRegistry().load_config(Config.load_dict({}))
    transport_registry.set_rpc_transport("default", rpc_transport)

    bus = lightbus.creation.create(transport_registry=transport_registry, plugins=[])
    try:
        assert m.call_count == 0

        bus.client.lazy_load_now()
        assert m.call_count == 1
    finally:
        bus.client.close()


def test_run_forever(dummy_bus: lightbus.path.BusPath, mocker, dummy_api):
    """A simple test to ensure run_forever executes without errors"""
    m = mocker.patch.object(dummy_bus.client, "_actually_run_forever")
    dummy_bus.client.run_forever()
    assert m.called


DECORATOR_HOOK_PAIRS = [
    ("on_start", "before_worker_start"),
    ("on_stop", "before_worker_start"),
    ("before_worker_start", "before_worker_start"),
    ("after_worker_stopped", "after_worker_stopped"),
    ("before_rpc_call", "before_rpc_call"),
    ("after_rpc_call", "after_rpc_call"),
    ("before_rpc_execution", "before_rpc_execution"),
    ("after_rpc_execution", "after_rpc_execution"),
    ("before_event_sent", "before_event_sent"),
    ("after_event_sent", "after_event_sent"),
    ("before_event_execution", "before_event_execution"),
    ("after_event_execution", "after_event_execution"),
]


@pytest.mark.parametrize("decorator,hook", DECORATOR_HOOK_PAIRS)
@pytest.mark.parametrize("before_plugins", [True, False], ids=["before-plugins", "after-plugins"])
@pytest.mark.asyncio
async def test_hook_decorator(dummy_bus: lightbus.path.BusPath, decorator, hook, before_plugins):
    count = 0
    decorator = getattr(dummy_bus.client, decorator)

    @decorator(before_plugins=before_plugins)
    def callback(*args, **kwargs):
        nonlocal count
        count += 1

    await dummy_bus.client._execute_hook(hook)

    assert count == 1


@pytest.mark.parametrize("decorator,hook", DECORATOR_HOOK_PAIRS)
@pytest.mark.parametrize("before_plugins", [True, False], ids=["before-plugins", "after-plugins"])
@pytest.mark.asyncio
async def test_hook_simple_call(dummy_bus: lightbus.path.BusPath, decorator, hook, before_plugins):
    count = 0
    decorator = getattr(dummy_bus.client, decorator)

    def callback(*args, **kwargs):
        nonlocal count
        count += 1

    decorator(callback, before_plugins=before_plugins)
    await dummy_bus.client._execute_hook(hook)

    assert count == 1


@pytest.mark.asyncio
async def test_exception_in_listener_shutdown(
    worker: Worker, queue_mocker: Type[BusQueueMockerContext]
):
    class TestException(Exception):
        pass

    def listener(*args, **kwargs):
        raise TestException()

    worker.bus.client.stop_loop = MagicMock()

    # Start the listener
    worker.bus.client.listen_for_events(
        events=[("my_api", "my_event")], listener=listener, listener_name="test"
    )
    with queue_mocker(worker.bus.client) as q:
        async with worker():
            consume_command = q.event.to_transport.commands.get(ConsumeEventsCommand)
            consume_command.destination_queue.put_nowait(
                EventMessage(api_name="my_api", event_name="my_event")
            )
            await asyncio.sleep(0.1)

            assert len(q.errors.put_items) == 1, f"Expected one error, got: {q.errors.put_items}"
            error: Error = q.errors.put_items[0]
            assert isinstance(error.value, TestException)
            assert worker.bus.client.stop_loop.called

            # Note that this hasn't actually shut the bus down, we'll test that in test_server_shutdown


def test_add_background_task(dummy_bus: lightbus.path.BusPath, event_loop):
    calls = 0

    async def test_coroutine():
        nonlocal calls
        while True:
            calls += 1
            if calls == 5:
                raise Exception("Intentional exception: stopping lightbus dummy bus from running")
            await asyncio.sleep(0.001)

    dummy_bus.client.add_background_task(test_coroutine())

    dummy_bus.client.run_forever()

    assert dummy_bus.client.exit_code
    assert dummy_bus.client.stop_loop.called

    assert calls == 5


def test_every(dummy_bus: lightbus.path.BusPath, event_loop):
    calls = 0

    @dummy_bus.client.every(seconds=0.001)
    async def test_coroutine():
        nonlocal calls
        while True:
            calls += 1
            if calls == 5:
                raise Exception("Intentional exception: stopping lightbus dummy bus from running")
            await asyncio.sleep(0.001)

    # SystemExit raised because test_coroutine throws an exception
    dummy_bus.client.run_forever()

    assert dummy_bus.client.exit_code

    assert calls == 5


def test_schedule(dummy_bus: lightbus.path.BusPath):
    import schedule

    calls = 0

    # TODO: This kind of 'throw exception to stop bus' hackery in the tests can be cleaned up
    @dummy_bus.client.schedule(schedule.every(0.001).seconds)
    async def test_coroutine():
        nonlocal calls
        while True:
            calls += 1
            if calls == 5:
                raise Exception("Intentional exception: stopping lightbus dummy bus from running")
            await asyncio.sleep(0.001)

    dummy_bus.client.run_forever()

    assert dummy_bus.client.exit_code

    assert calls == 5
