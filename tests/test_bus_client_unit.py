import jsonschema
import pytest

import lightbus
import lightbus.creation
import lightbus.path
from lightbus import Schema, RpcMessage, ResultMessage, EventMessage, BusClient
from lightbus.config import Config
from lightbus.exceptions import (
    UnknownApi,
    EventNotFound,
    InvalidEventArguments,
    InvalidEventListener,
    TransportNotFound,
    InvalidName,
    ValidationError,
)
from lightbus.utilities.async import get_event_loop

pytestmark = pytest.mark.unit


@pytest.mark.run_loop
async def test_fire_event_api_doesnt_exist(dummy_bus: lightbus.path.BusPath):
    with pytest.raises(UnknownApi):
        await dummy_bus.client.fire_event("non_existent_api", "event")


@pytest.mark.run_loop
async def test_fire_event_event_doesnt_exist(dummy_bus: lightbus.path.BusPath, dummy_api):
    with pytest.raises(EventNotFound):
        await dummy_bus.client.fire_event("my.dummy", "bad_event")


@pytest.mark.run_loop
async def test_fire_event_bad_event_arguments(dummy_bus: lightbus.path.BusPath, dummy_api):
    with pytest.raises(InvalidEventArguments):
        await dummy_bus.client.fire_event("my.dummy", "my_event", kwargs={"bad_arg": "value"})


@pytest.mark.run_loop
async def test_listen_for_event_non_callable(dummy_bus: lightbus.path.BusPath):
    with pytest.raises(InvalidEventListener):
        await dummy_bus.client.listen_for_event("my.dummy", "my_event", listener=123)


@pytest.mark.run_loop
async def test_listen_for_event_no_args(dummy_bus: lightbus.path.BusPath):
    with pytest.raises(InvalidEventListener):
        await dummy_bus.client.listen_for_event("my.dummy", "my_event", listener=lambda: None)


@pytest.mark.run_loop
async def test_listen_for_event_no_positional_args(dummy_bus: lightbus.path.BusPath):
    with pytest.raises(InvalidEventListener):
        await dummy_bus.client.listen_for_event("my.dummy", "my_event", listener=lambda **kw: None)


@pytest.mark.run_loop
async def test_listen_for_event_one_positional_arg(dummy_bus: lightbus.path.BusPath):
    await dummy_bus.client.listen_for_event(
        "my.dummy", "my_event", listener=lambda event_message: None
    )


@pytest.mark.run_loop
async def test_listen_for_event_two_positional_args(dummy_bus: lightbus.path.BusPath):
    await dummy_bus.client.listen_for_event(
        "my.dummy", "my_event", listener=lambda event_message, other: None
    )


@pytest.mark.run_loop
async def test_listen_for_event_variable_positional_args(dummy_bus: lightbus.path.BusPath):
    await dummy_bus.client.listen_for_event("my.dummy", "my_event", listener=lambda *a: None)


@pytest.mark.run_loop
async def test_listen_for_event_starts_with_underscore(dummy_bus: lightbus.path.BusPath):
    with pytest.raises(InvalidName):
        await dummy_bus.client.listen_for_event(
            "my.dummy", "_my_event", listener=lambda *a, **kw: None
        )


@pytest.mark.run_loop
async def test_fire_event_starts_with_underscore(dummy_bus: lightbus.path.BusPath, dummy_api):
    with pytest.raises(InvalidName):
        await dummy_bus.client.fire_event("my.dummy", "_my_event")


@pytest.mark.run_loop
async def test_call_rpc_remote_starts_with_underscore(dummy_bus: lightbus.path.BusPath):
    with pytest.raises(InvalidName):
        await dummy_bus.client.call_rpc_remote("my.dummy", "_my_event")


@pytest.mark.run_loop
async def test_call_rpc_local_starts_with_underscore(dummy_bus: lightbus.path.BusPath, dummy_api):
    with pytest.raises(InvalidName):
        await dummy_bus.client.call_rpc_local("my.dummy", "_my_event")


@pytest.mark.run_loop
async def test_listen_for_event_empty_name(dummy_bus: lightbus.path.BusPath):
    with pytest.raises(InvalidName):
        await dummy_bus.client.listen_for_event("my.dummy", "_my_event", listener=lambda *a: None)


@pytest.mark.run_loop
async def test_fire_event_empty_name(dummy_bus: lightbus.path.BusPath, dummy_api):
    with pytest.raises(InvalidName):
        await dummy_bus.client.fire_event("my.dummy", "_my_event")


@pytest.mark.run_loop
async def test_call_rpc_remote_empty_name(dummy_bus: lightbus.path.BusPath):
    with pytest.raises(InvalidName):
        await dummy_bus.client.call_rpc_remote("my.dummy", "_my_event")


@pytest.mark.run_loop
async def test_call_rpc_local_empty_name(dummy_bus: lightbus.path.BusPath, dummy_api):
    with pytest.raises(InvalidName):
        await dummy_bus.client.call_rpc_local("my.dummy", "_my_event")


@pytest.mark.run_loop
async def test_no_transport(loop):
    # No transports configured for any relevant api
    config = Config.load_dict(
        {"apis": {"default": {"event_transport": {"redis": {}}}}}, set_defaults=False
    )
    client = lightbus.BusClient(config=config)
    with pytest.raises(TransportNotFound):
        await client.call_rpc_remote("my_api", "test", kwargs={}, options={})


@pytest.mark.run_loop
async def test_no_transport_type(loop):
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
        schema = Schema(schema_transport=None)
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


def test_setup_transports_opened(loop, mocker):
    rpc_transport = lightbus.DebugRpcTransport()

    # TODO: There has to be a cleaner way of patching a coroutine.
    #       Do a global search for 'dummy_coroutine' if you find it
    async def dummy_coroutine(*args, **kwargs):
        pass

    m = mocker.patch.object(rpc_transport, "open", autospec=True, return_value=dummy_coroutine())

    lightbus.creation.create(
        rpc_transport=rpc_transport,
        schema_transport=lightbus.DebugSchemaTransport(),
        loop=loop,
        plugins={},
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
@pytest.mark.run_loop
async def test_hook_decorator(dummy_bus: lightbus.path.BusPath, decorator, hook, before_plugins):
    count = 0
    decorator = getattr(dummy_bus.client, decorator)

    @decorator(before_plugins=before_plugins)
    def callback(*args, **kwargs):
        nonlocal count
        count += 1

    await dummy_bus.client._plugin_hook(hook)

    assert count == 1


@pytest.mark.parametrize("decorator,hook", DECORATOR_HOOK_PAIRS)
@pytest.mark.parametrize("before_plugins", [True, False], ids=["before-plugins", "after-plugins"])
@pytest.mark.run_loop
async def test_hook_simple_call(dummy_bus: lightbus.path.BusPath, decorator, hook, before_plugins):
    count = 0
    decorator = getattr(dummy_bus.client, decorator)

    def callback(*args, **kwargs):
        nonlocal count
        count += 1

    decorator(callback, before_plugins=before_plugins)
    await dummy_bus.client._plugin_hook(hook)

    assert count == 1
