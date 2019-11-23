import asyncio
import threading
from asyncio import BaseEventLoop

import janus
import jsonschema
from unittest import mock
import pytest

import lightbus
import lightbus.creation
import lightbus.path
from lightbus import Schema, RpcMessage, ResultMessage, EventMessage, BusClient
from lightbus.client_worker import run_in_worker_thread
from lightbus.config import Config
from lightbus.config.structure import OnError
from lightbus.exceptions import (
    UnknownApi,
    EventNotFound,
    InvalidEventArguments,
    InvalidEventListener,
    TransportNotFound,
    InvalidName,
    ValidationError,
    SuddenDeathException,
    WorkerDeadlock,
)
from lightbus.transports.base import TransportRegistry
from lightbus.utilities.async_tools import cancel, run_user_provided_callable

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
        await dummy_bus.client._call_rpc_local("my.dummy", "_my_event")


@pytest.mark.asyncio
async def test_consume_rpcs_with_transport_error(
    mocker, dummy_bus: lightbus.path.BusPath, dummy_api
):
    class TestException(Exception):
        pass

    async def co(e=None):
        if e:
            raise e

    dummy_bus.client.register_api(dummy_api)

    mocker.patch.object(dummy_bus.client, "_call_rpc_local", return_value=co(TestException()))
    fut = asyncio.Future()
    fut.set_result(None)
    send_result = mocker.patch.object(dummy_bus.client, "send_result", return_value=fut)

    task = asyncio.ensure_future(
        dummy_bus.client._consume_rpcs_with_transport(
            rpc_transport=dummy_bus.client.transport_registry.get_rpc_transport("default"), apis=[]
        )
    )

    # Wait for the consumer to send the result
    for _ in range(0, 10):
        if send_result.called:
            break
        await asyncio.sleep(0.1)
    await cancel(task)

    assert send_result.called
    result_kwargs = send_result.call_args[1]
    assert result_kwargs["result_message"].error
    assert result_kwargs["result_message"].result
    assert result_kwargs["result_message"].trace


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
        await dummy_bus.client._call_rpc_local("my.dummy", "_my_event")


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


# Validation


@pytest.yield_fixture()
def create_bus_client_with_unhappy_schema(mocker, dummy_bus):
    """Schema which always fails to validate"""

    # Note we default to strict_validation for most tests
    def create_bus_client_with_unhappy_schema(validate=True, strict_validation=True):
        # Use the base transport as a dummy, it only needs to have a
        # close() method on it in order to keep the client.close() method happy
        schema = Schema(schema_transport=lightbus.Transport())
        # Fake loading of remote schemas from schema transport
        schema._remote_schemas = {}
        config = Config.load_dict(
            {"apis": {"default": {"validate": validate, "strict_validation": strict_validation}}}
        )
        fake_schema = {"parameters": {"p": {}}, "response": {}}
        mocker.patch.object(schema, "get_rpc_schema", autospec=True, return_value=fake_schema),
        mocker.patch.object(schema, "get_event_schema", autospec=True, return_value=fake_schema),
        # Make sure the test api named "api" has a schema, otherwise strict_validation
        # will fail it
        schema.local_schemas["api"] = fake_schema
        mocker.patch(
            "jsonschema.validate", autospec=True, side_effect=ValidationError("test error")
        ),
        dummy_bus.client.schema = schema
        dummy_bus.client.config = config

        return dummy_bus.client

    return create_bus_client_with_unhappy_schema


def test_rpc_validate_incoming(create_bus_client_with_unhappy_schema):
    client: BusClient = create_bus_client_with_unhappy_schema()

    message = RpcMessage(api_name="api", procedure_name="proc", kwargs={"p": 1})
    with pytest.raises(ValidationError):
        client._validate(message, direction="outgoing", api_name="api", procedure_name="proc")
    jsonschema.validate.assert_called_with({"p": 1}, {"p": {}})


def test_result_validate(create_bus_client_with_unhappy_schema):
    client: BusClient = create_bus_client_with_unhappy_schema()

    message = ResultMessage(result="123", rpc_message_id="123")
    with pytest.raises(ValidationError):
        client._validate(message, direction="outgoing", api_name="api", procedure_name="proc")
    jsonschema.validate.assert_called_with("123", {})


def test_event_validate(create_bus_client_with_unhappy_schema):
    client: BusClient = create_bus_client_with_unhappy_schema()

    message = EventMessage(api_name="api", event_name="proc", kwargs={"p": 1})
    with pytest.raises(ValidationError):
        client._validate(message, direction="outgoing", api_name="api", procedure_name="proc")
    jsonschema.validate.assert_called_with({"p": 1}, {"p": {}})


def test_validate_disabled(create_bus_client_with_unhappy_schema):
    client: BusClient = create_bus_client_with_unhappy_schema(validate=False)

    message = RpcMessage(api_name="api", procedure_name="proc", kwargs={"p": 1})
    client._validate(message, direction="outgoing", api_name="api", procedure_name="proc")


def test_validate_non_strict(create_bus_client_with_unhappy_schema):
    client: BusClient = create_bus_client_with_unhappy_schema(strict_validation=False)

    # Using 'missing_api', which there is no schema for, so it
    # should validate just fine as strict_validation=False
    # (albeit with a warning)
    message = RpcMessage(api_name="missing_api", procedure_name="proc", kwargs={"p": 1})
    client._validate(message, direction="outgoing", api_name="missing_api", procedure_name="proc")


def test_validate_strict_missing_api(create_bus_client_with_unhappy_schema):
    client: BusClient = create_bus_client_with_unhappy_schema(strict_validation=True)

    # Using 'missing_api', which there is no schema for, so it
    # raise an error as strict_validation=True
    message = RpcMessage(api_name="missing_api", procedure_name="proc", kwargs={"p": 1})
    with pytest.raises(UnknownApi):
        client._validate(
            message, direction="outgoing", api_name="missing_api", procedure_name="proc"
        )


def test_validate_incoming_disabled(create_bus_client_with_unhappy_schema):
    client: BusClient = create_bus_client_with_unhappy_schema(validate={"incoming": False})

    message = RpcMessage(api_name="api", procedure_name="proc", kwargs={"p": 1})
    client._validate(message, direction="incoming", api_name="api", procedure_name="proc")


def test_validate_incoming_enabled(create_bus_client_with_unhappy_schema):
    client: BusClient = create_bus_client_with_unhappy_schema(validate={"incoming": True})

    message = RpcMessage(api_name="api", procedure_name="proc", kwargs={"p": 1})
    with pytest.raises(ValidationError):
        client._validate(message, direction="incoming", api_name="api", procedure_name="proc")


def test_validate_outgoing_disabled(create_bus_client_with_unhappy_schema):
    client: BusClient = create_bus_client_with_unhappy_schema(validate={"outgoing": False})

    message = RpcMessage(api_name="api", procedure_name="proc", kwargs={"p": 1})
    client._validate(message, direction="outgoing", api_name="api", procedure_name="proc")


def test_validate_outgoing_enabled(create_bus_client_with_unhappy_schema):
    client: BusClient = create_bus_client_with_unhappy_schema(validate={"outgoing": True})

    message = RpcMessage(api_name="api", procedure_name="proc", kwargs={"p": 1})
    with pytest.raises(ValidationError):
        client._validate(message, direction="outgoing", api_name="api", procedure_name="proc")


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
async def test_exception_in_listener_shutdown(dummy_bus: lightbus.path.BusPath, caplog):
    dummy_bus.client.config.api("default").on_error = OnError.SHUTDOWN

    # This is normally only set on server startup, so set it here so it can be used
    dummy_bus.client._server_shutdown_queue = janus.Queue()

    def listener(*args, **kwargs):
        raise Exception()

    # Start the listener
    dummy_bus.client.listen_for_events(
        events=[("my_company.auth", "user_registered")], listener=listener, listener_name="test"
    )

    await dummy_bus.client._setup_server()

    # Check we have something in the shutdown queue
    await asyncio.wait_for(dummy_bus.client._server_shutdown_queue.async_q.get(), timeout=5)

    # Note that this hasn't actually shut the bus down, we'll test that in test_server_shutdown


@pytest.mark.asyncio
async def test_exception_in_listener_stop_listener(dummy_bus: lightbus.path.BusPath, loop, caplog):
    dummy_bus.client.config.api("default").on_error = OnError.STOP_LISTENER

    class SomeException(Exception):
        pass

    def listener(*args, **kwargs):
        raise SomeException()

    dummy_bus.client.listen_for_events(
        events=[("my_company.auth", "user_registered")], listener=listener, listener_name="test"
    )

    await dummy_bus.client._setup_server()

    # Don't let the event loop be stopped as it is needed to run the tests!
    # (although stop() shouldn't actually be called here)
    with mock.patch.object(loop, "stop") as m:
        # Dummy event transport fires events every 0.1 seconds
        await asyncio.sleep(0.15)
        assert not m.called

    log_levels = {r.levelname for r in caplog.records}
    # Ensure the error was logged
    assert "ERROR" in log_levels


@pytest.mark.asyncio
async def test_exception_in_listener_ignore(dummy_bus: lightbus.path.BusPath, loop, caplog):
    dummy_bus.client.config.api("default").on_error = OnError.IGNORE

    class SomeException(Exception):
        pass

    def listener(*args, **kwargs):
        raise SomeException()

    dummy_bus.client.listen_for_events(
        events=[("my_company.auth", "user_registered")], listener=listener, listener_name="test"
    )

    await dummy_bus.client._setup_server()

    # Don't let the event loop be stopped as it is needed to run the tests!
    # (although stop() shouldn't actually be called here)
    with mock.patch.object(loop, "stop") as m:
        # Dummy event transport fires events every 0.1 seconds
        await asyncio.sleep(0.15)
        assert not m.called

    log_levels = {r.levelname for r in caplog.records}
    # Ensure the error was logged
    assert "ERROR" in log_levels


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


def test_schedule(dummy_bus: lightbus.path.BusPath, event_loop):
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


@pytest.mark.asyncio
async def test_server_shutdown(dummy_bus: lightbus.path.BusPath, caplog):
    thread = threading.Thread(target=dummy_bus.client.run_forever, name="TestLightbusServerThread")
    thread.start()

    await asyncio.sleep(0.1)
    dummy_bus.client.shutdown_server(exit_code=1)
    await asyncio.sleep(1)

    assert (
        not dummy_bus.client.worker._thread.is_alive()
    ), "Bus worker thread is still running but should have stopped"
    # Make sure will pull out any exceptions
    dummy_bus.client.worker._thread.join()


@pytest.mark.asyncio
async def test_worker_deadlock(dummy_bus: lightbus.path.BusPath):
    @run_in_worker_thread(worker=dummy_bus.client.worker)
    async def fun1():
        print("fun1")

    @run_in_worker_thread(worker=dummy_bus.client.worker)
    async def fun2():
        await run_user_provided_callable(fun1, args=[], kwargs={}, bus_client=dummy_bus.client)

    with pytest.raises(WorkerDeadlock) as exc_info:
        await fun2()

    # Check we have the stack trace for both calls printed out
    assert "await fun2()" in str(exc_info.value)
    assert "run_user_provided_callable(fun1" in str(exc_info.value)
