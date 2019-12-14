from typing import NamedTuple

import jsonschema
import pytest

import lightbus
from lightbus import Schema, BusClient, RpcMessage, ResultMessage, EventMessage
from lightbus.client.validator import validate_outgoing, validate_incoming
from lightbus.config import Config
from lightbus.exceptions import ValidationError, UnknownApi
from lightbus.transports.pool import TransportPool

pytestmark = pytest.mark.unit


@pytest.yield_fixture()
def create_bus_client_with_unhappy_schema(mocker, dummy_bus):
    """Schema which always fails to validate"""

    # Note we default to strict_validation for most tests
    def create_bus_client_with_unhappy_schema(validate=True, strict_validation=True):
        # Use the base transport as a dummy, it only needs to have a
        # close() method on it in order to keep the client.close() method happy
        schema = Schema(
            schema_transport_pool=TransportPool(
                transport_class=lightbus.Transport,
                config=None,
                transport_config=NamedTuple("DummyTransportConfig")(),
            )
        )
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
        validate_outgoing(config=client.config, schema=client.schema, message=message)
    jsonschema.validate.assert_called_with({"p": 1}, {"p": {}})


def test_result_validate(create_bus_client_with_unhappy_schema):
    client: BusClient = create_bus_client_with_unhappy_schema()

    message = ResultMessage(
        result="123", rpc_message_id="123", api_name="api", procedure_name="proc"
    )
    with pytest.raises(ValidationError):
        validate_outgoing(config=client.config, schema=client.schema, message=message)
    jsonschema.validate.assert_called_with("123", {})


def test_event_validate(create_bus_client_with_unhappy_schema):
    client: BusClient = create_bus_client_with_unhappy_schema()

    message = EventMessage(api_name="api", event_name="proc", kwargs={"p": 1})
    with pytest.raises(ValidationError):
        validate_outgoing(config=client.config, schema=client.schema, message=message)
    jsonschema.validate.assert_called_with({"p": 1}, {"p": {}})


def test_validate_disabled(create_bus_client_with_unhappy_schema):
    client: BusClient = create_bus_client_with_unhappy_schema(validate=False)

    message = RpcMessage(api_name="api", procedure_name="proc", kwargs={"p": 1})
    validate_outgoing(config=client.config, schema=client.schema, message=message)


def test_validate_non_strict(create_bus_client_with_unhappy_schema):
    client: BusClient = create_bus_client_with_unhappy_schema(strict_validation=False)

    # Using 'missing_api', which there is no schema for, so it
    # should validate just fine as strict_validation=False
    # (albeit with a warning)
    message = RpcMessage(api_name="missing_api", procedure_name="proc", kwargs={"p": 1})
    validate_outgoing(config=client.config, schema=client.schema, message=message)


def test_validate_strict_missing_api(create_bus_client_with_unhappy_schema):
    client: BusClient = create_bus_client_with_unhappy_schema(strict_validation=True)

    # Using 'missing_api', which there is no schema for, so it
    # raise an error as strict_validation=True
    message = RpcMessage(api_name="missing_api", procedure_name="proc", kwargs={"p": 1})
    with pytest.raises(UnknownApi):
        validate_outgoing(config=client.config, schema=client.schema, message=message)


def test_validate_incoming_disabled(create_bus_client_with_unhappy_schema):
    client: BusClient = create_bus_client_with_unhappy_schema(validate={"incoming": False})

    message = RpcMessage(api_name="api", procedure_name="proc", kwargs={"p": 1})
    validate_incoming(config=client.config, schema=client.schema, message=message)


def test_validate_incoming_enabled(create_bus_client_with_unhappy_schema):
    client: BusClient = create_bus_client_with_unhappy_schema(validate={"incoming": True})

    message = RpcMessage(api_name="api", procedure_name="proc", kwargs={"p": 1})
    with pytest.raises(ValidationError):
        validate_incoming(config=client.config, schema=client.schema, message=message)


def test_validate_outgoing_disabled(create_bus_client_with_unhappy_schema):
    client: BusClient = create_bus_client_with_unhappy_schema(validate={"outgoing": False})

    message = RpcMessage(api_name="api", procedure_name="proc", kwargs={"p": 1})
    validate_outgoing(config=client.config, schema=client.schema, message=message)


def test_validate_outgoing_enabled(create_bus_client_with_unhappy_schema):
    client: BusClient = create_bus_client_with_unhappy_schema(validate={"outgoing": True})

    message = RpcMessage(api_name="api", procedure_name="proc", kwargs={"p": 1})
    with pytest.raises(ValidationError):
        validate_outgoing(config=client.config, schema=client.schema, message=message)
