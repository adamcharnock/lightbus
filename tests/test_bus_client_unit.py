import asyncio

import jsonschema
from unittest import mock
import pytest

import lightbus
import lightbus.creation
import lightbus.path
from lightbus import Schema, RpcMessage, ResultMessage, EventMessage, BusClient
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
)

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
        await dummy_bus.client.listen_for_event("my.dummy", "my_event", listener=123)


@pytest.mark.asyncio
async def test_listen_for_event_no_args(dummy_bus: lightbus.path.BusPath):
    with pytest.raises(InvalidEventListener):
        await dummy_bus.client.listen_for_event("my.dummy", "my_event", listener=lambda: None)


@pytest.mark.asyncio
async def test_listen_for_event_no_positional_args(dummy_bus: lightbus.path.BusPath):
    with pytest.raises(InvalidEventListener):
        await dummy_bus.client.listen_for_event("my.dummy", "my_event", listener=lambda **kw: None)


@pytest.mark.asyncio
async def test_listen_for_event_one_positional_arg(dummy_bus: lightbus.path.BusPath):
    await dummy_bus.client.listen_for_event(
        "my.dummy", "my_event", listener=lambda event_message: None
    )


@pytest.mark.asyncio
async def test_listen_for_event_two_positional_args(dummy_bus: lightbus.path.BusPath):
    await dummy_bus.client.listen_for_event(
        "my.dummy", "my_event", listener=lambda event_message, other: None
    )


@pytest.mark.asyncio
async def test_listen_for_event_variable_positional_args(dummy_bus: lightbus.path.BusPath):
    await dummy_bus.client.listen_for_event("my.dummy", "my_event", listener=lambda *a: None)


@pytest.mark.asyncio
async def test_listen_for_event_starts_with_underscore(dummy_bus: lightbus.path.BusPath):
    with pytest.raises(InvalidName):
        await dummy_bus.client.listen_for_event(
            "my.dummy", "_my_event", listener=lambda *a, **kw: None
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
        await dummy_bus.client.call_rpc_local("my.dummy", "_my_event")


@pytest.mark.asyncio
async def test_listen_for_event_empty_name(dummy_bus: lightbus.path.BusPath):
    with pytest.raises(InvalidName):
        await dummy_bus.client.listen_for_event("my.dummy", "_my_event", listener=lambda *a: None)


@pytest.mark.asyncio
async def test_fire_event_empty_name(dummy_bus: lightbus.path.BusPath, dummy_api):
    dummy_bus.client.register_api(dummy_api)
    with pytest.raises(InvalidName):
        await dummy_bus.client.fire_event("my.dummy", "_my_event")


@pytest.mark.asyncio
async def test_call_rpc_remote_empty_name(dummy_bus: lightbus.path.BusPath):
    with pytest.raises(InvalidName):
        await dummy_bus.client.call_rpc_remote("my.dummy", "_my_event")


@pytest.mark.asyncio
async def test_call_rpc_local_empty_name(dummy_bus: lightbus.path.BusPath, dummy_api):
    dummy_bus.client.register_api(dummy_api)
    with pytest.raises(InvalidName):
        await dummy_bus.client.call_rpc_local("my.dummy", "_my_event")


@pytest.mark.asyncio
async def test_no_transport():
    # No transports configured for any relevant api
    config = Config.load_dict(
        {"apis": {"default": {"event_transport": {"redis": {}}}}}, set_defaults=False
    )
    client = lightbus.BusClient(config=config)
    with pytest.raises(TransportNotFound):
        await client.call_rpc_remote("my_api", "test", kwargs={}, options={})


@pytest.mark.asyncio
async def test_no_transport_type():
    # Transports configured, but the wrong type of transport
    config = Config.load_dict(
        {
            "apis": {
                "default": {"event_transport": {"redis": {}}},
                "my_api": {"event_transport": {"redis": {}}},
            }
        },
        set_defaults=False,
    )
    client = lightbus.BusClient(config=config)
    with pytest.raises(TransportNotFound):
        await client.call_rpc_remote("my_api", "test", kwargs={}, options={})


# Validation


@pytest.yield_fixture()
def create_bus_client_with_unhappy_schema(mocker, dummy_bus):
    """Schema which always fails to validate"""

    # Note we default to strict_validation for most tests
    def create_bus_client_with_unhappy_schema(validate=True, strict_validation=True):
        # Use the base transport as a dummy, it only needs to have a
        # close() method on it in order to keep the client.close() method happy
        schema = Schema(schema_transport=lightbus.Transport())
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

    lightbus.creation.create(
        rpc_transport=rpc_transport, schema_transport=lightbus.DebugSchemaTransport(), plugins={}
    )
    assert m.call_count == 1


def test_run_forever(dummy_bus: lightbus.path.BusPath, mocker, dummy_api):
    """A simple test to ensure run_forever executes without errors"""
    m = mocker.patch.object(dummy_bus.client, "_actually_run_forever")
    dummy_bus.client.run_forever()
    assert m.called


def test_register_listener_context_manager(dummy_bus: lightbus.path.BusPath):
    client = dummy_bus.client
    assert len(client._listeners) == 0
    with client._register_listener([("api_name", "event_name")]):
        assert client._listeners[("api_name", "event_name")] == 1
        with client._register_listener([("api_name", "event_name")]):
            assert client._listeners[("api_name", "event_name")] == 2
        assert client._listeners[("api_name", "event_name")] == 1
    assert len(client._listeners) == 0


DECORATOR_HOOK_PAIRS = [
    ("on_start", "before_server_start"),
    ("on_stop", "before_server_start"),
    ("before_server_start", "before_server_start"),
    ("after_server_stopped", "after_server_stopped"),
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
async def test_exception_in_listener_shutdown(dummy_bus: lightbus.path.BusPath, loop, caplog):
    dummy_bus.client.config.api("default").on_error = OnError.SHUTDOWN

    class SomeException(Exception):
        pass

    def listener(*args, **kwargs):
        raise SomeException()

    task = await dummy_bus.client.listen_for_events(
        events=[("my_company.auth", "user_registered")], listener=listener
    )

    # Don't let the event loop be stopped as it is needed to run the tests!
    with mock.patch.object(loop, "stop") as m:
        # Dummy event transport fires events every 0.1 seconds
        await asyncio.sleep(0.15)
        assert m.called

    assert loop.lightbus_exit_code
    del loop.lightbus_exit_code  # Delete to stop lightbus actually quitting

    # Close the bus to force tasks to be cleaned up
    # (This would have happened normally when the event loop was stopped,
    # but we prevented that my mocking stop() above)
    await dummy_bus.client.close_async()

    log_levels = {r.levelname for r in caplog.records}
    # Ensure the error was logged
    assert "ERROR" in log_levels


@pytest.mark.asyncio
async def test_exception_in_listener_stop_listener(dummy_bus: lightbus.path.BusPath, loop, caplog):
    dummy_bus.client.config.api("default").on_error = OnError.STOP_LISTENER

    class SomeException(Exception):
        pass

    def listener(*args, **kwargs):
        raise SomeException()

    task = await dummy_bus.client.listen_for_events(
        events=[("my_company.auth", "user_registered")], listener=listener
    )

    # Don't let the event loop be stopped as it is needed to run the tests!
    # (although stop() shouldn't actually be called here)
    with mock.patch.object(loop, "stop") as m:
        # Dummy event transport fires events every 0.1 seconds
        await asyncio.sleep(0.15)
        assert not m.called

    # Listener task has stopped
    assert task.done()

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

    task = await dummy_bus.client.listen_for_events(
        events=[("my_company.auth", "user_registered")], listener=listener
    )

    # Don't let the event loop be stopped as it is needed to run the tests!
    # (although stop() shouldn't actually be called here)
    with mock.patch.object(loop, "stop") as m:
        # Dummy event transport fires events every 0.1 seconds
        await asyncio.sleep(0.15)
        assert not m.called

    # Listener task is still running
    assert not task.done()

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
    dummy_bus.client._run_forever(consume_rpcs=False)
    dummy_bus.client.close()

    assert dummy_bus.client.loop.lightbus_exit_code
    del dummy_bus.client.loop.lightbus_exit_code  # Delete to stop lightbus actually quitting

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

    dummy_bus.client._run_forever(consume_rpcs=False)
    dummy_bus.client.close()

    assert dummy_bus.client.loop.lightbus_exit_code
    del dummy_bus.client.loop.lightbus_exit_code  # Delete to stop lightbus actually quitting

    assert calls == 5


def test_schedule(dummy_bus: lightbus.path.BusPath, event_loop):
    import schedule

    calls = 0

    @dummy_bus.client.schedule(schedule.every(0.001).seconds)
    async def test_coroutine():
        nonlocal calls
        while True:
            calls += 1
            if calls == 5:
                raise Exception("Intentional exception: stopping lightbus dummy bus from running")
            await asyncio.sleep(0.001)

    dummy_bus.client._run_forever(consume_rpcs=False)
    dummy_bus.client.close()

    assert dummy_bus.client.loop.lightbus_exit_code
    del dummy_bus.client.loop.lightbus_exit_code  # Delete to stop lightbus actually quitting

    assert calls == 5
