import jsonschema
import pytest
from jsonschema import ValidationError

import lightbus
from lightbus import Schema, RpcMessage, ResultMessage, EventMessage, BusClient
from lightbus.config import Config
from lightbus.exceptions import UnknownApi, EventNotFound, InvalidEventArguments, InvalidEventListener, \
    TransportNotFound, InvalidName

pytestmark = pytest.mark.unit


@pytest.mark.run_loop
async def test_fire_event_api_doesnt_exist(dummy_bus: lightbus.BusNode):
    with pytest.raises(UnknownApi):
        await dummy_bus.bus_client.fire_event('non_existent_api', 'event')


@pytest.mark.run_loop
async def test_fire_event_event_doesnt_exist(dummy_bus: lightbus.BusNode, dummy_api):
    with pytest.raises(EventNotFound):
        await dummy_bus.bus_client.fire_event('my.dummy', 'bad_event')


@pytest.mark.run_loop
async def test_fire_event_bad_event_arguments(dummy_bus: lightbus.BusNode, dummy_api):
    with pytest.raises(InvalidEventArguments):
        await dummy_bus.bus_client.fire_event('my.dummy', 'my_event', kwargs={'bad_arg': 'value'})


@pytest.mark.run_loop
async def test_listen_for_event_non_callable(dummy_bus: lightbus.BusNode):
    with pytest.raises(InvalidEventListener):
        await dummy_bus.bus_client.listen_for_event('my.dummy', 'my_event', listener=123)


@pytest.mark.run_loop
async def test_listen_for_event_starts_with_underscore(dummy_bus: lightbus.BusNode):
    with pytest.raises(InvalidName):
        await dummy_bus.bus_client.listen_for_event('my.dummy', '_my_event', listener=lambda: None)


@pytest.mark.run_loop
async def test_fire_event_starts_with_underscore(dummy_bus: lightbus.BusNode, dummy_api):
    with pytest.raises(InvalidName):
        await dummy_bus.bus_client.fire_event('my.dummy', '_my_event')


@pytest.mark.run_loop
async def test_call_rpc_remote_starts_with_underscore(dummy_bus: lightbus.BusNode):
    with pytest.raises(InvalidName):
        await dummy_bus.bus_client.call_rpc_remote('my.dummy', '_my_event')


@pytest.mark.run_loop
async def test_call_rpc_local_starts_with_underscore(dummy_bus: lightbus.BusNode, dummy_api):
    with pytest.raises(InvalidName):
        await dummy_bus.bus_client.call_rpc_local('my.dummy', '_my_event')


@pytest.mark.run_loop
async def test_listen_for_event_empty_name(dummy_bus: lightbus.BusNode):
    with pytest.raises(InvalidName):
        await dummy_bus.bus_client.listen_for_event('my.dummy', '', listener=lambda: None)


@pytest.mark.run_loop
async def test_fire_event_empty_name(dummy_bus: lightbus.BusNode, dummy_api):
    with pytest.raises(InvalidName):
        await dummy_bus.bus_client.fire_event('my.dummy', '')


@pytest.mark.run_loop
async def test_call_rpc_remote_empty_name(dummy_bus: lightbus.BusNode):
    with pytest.raises(InvalidName):
        await dummy_bus.bus_client.call_rpc_remote('my.dummy', '')


@pytest.mark.run_loop
async def test_call_rpc_local_empty_name(dummy_bus: lightbus.BusNode, dummy_api):
    with pytest.raises(InvalidName):
        await dummy_bus.bus_client.call_rpc_local('my.dummy', '')


@pytest.mark.run_loop
async def test_no_transport(loop):
    # No transports configured for any relevant api
    config = Config.load_dict({
        'apis': {
            # TODO: This needs moving out of the apis config section
            'default': {'schema_transport': {'redis': {}}},
        }
    })
    bus_client = lightbus.BusClient(config=config)
    with pytest.raises(TransportNotFound):
        await bus_client.call_rpc_remote('my_api', 'test', kwargs={}, options={})


@pytest.mark.run_loop
async def test_no_transport_type(loop):
    # Transports configured, but the wrong type of transport
    config = Config.load_dict({
        'apis': {
            # TODO: This needs moving out of the apis config section
            'default': {'schema_transport': {'redis': {}}},
            'my_api': {
                'event_transport': {'redis': {}},
            }
        }
    })
    bus_client = lightbus.BusClient(config=config)
    with pytest.raises(TransportNotFound):
        await bus_client.call_rpc_remote('my_api', 'test', kwargs={}, options={})


# Validation


@pytest.yield_fixture()
def create_bus_client_with_unhappy_schema(mocker, dummy_bus):
    """Schema which always fails to validate"""
    def create_bus_client_with_unhappy_schema(validate=True, strict_validation=True):
        schema = Schema(schema_transport=None)
        config = Config.load_dict({
            'apis': {
                'default': {'validate': validate, 'strict_validation': strict_validation}
            }
        })
        fake_schema = {'parameters': {'p': {}}, 'response': {}}
        mocker.patch.object(schema, 'get_rpc_schema', autospec=True, return_value=fake_schema),
        mocker.patch.object(schema, 'get_event_schema', autospec=True, return_value=fake_schema),
        mocker.patch('jsonschema.validate', autospec=True, side_effect=ValidationError('test error')),
        dummy_bus.bus_client.schema = schema
        dummy_bus.bus_client.config = config
        return dummy_bus.bus_client
    return create_bus_client_with_unhappy_schema


def test_rpc_validate_incoming(create_bus_client_with_unhappy_schema):
    client: BusClient = create_bus_client_with_unhappy_schema()

    message = RpcMessage(api_name='api', procedure_name='proc', kwargs={'p': 1})
    with pytest.raises(jsonschema.ValidationError):
        client._validate(message, direction='outgoing', api_name='api', procedure_name='proc')
    jsonschema.validate.assert_called_with({'p': 1}, {'p': {}})


def test_result_validate(create_bus_client_with_unhappy_schema):
    client: BusClient = create_bus_client_with_unhappy_schema()

    message = ResultMessage(result='123', rpc_id='123')
    with pytest.raises(jsonschema.ValidationError):
        client._validate(message, direction='outgoing', api_name='api', procedure_name='proc')
    jsonschema.validate.assert_called_with('123', {})


def test_event_validate(create_bus_client_with_unhappy_schema):
    client: BusClient = create_bus_client_with_unhappy_schema()

    message = EventMessage(api_name='api', event_name='proc', kwargs={'p': 1})
    with pytest.raises(jsonschema.ValidationError):
        client._validate(message, direction='outgoing', api_name='api', procedure_name='proc')
    jsonschema.validate.assert_called_with({'p': 1}, {'p': {}})


def test_validate_disabled(create_bus_client_with_unhappy_schema):
    client: BusClient = create_bus_client_with_unhappy_schema(validate=False)

    message = RpcMessage(api_name='api', procedure_name='proc', kwargs={'p': 1})
    client._validate(message, direction='outgoing', api_name='api', procedure_name='proc')


def test_validate_non_strict(create_bus_client_with_unhappy_schema):
    client: BusClient = create_bus_client_with_unhappy_schema(strict_validation=False)

    message = RpcMessage(api_name='api', procedure_name='proc', kwargs={'p': 1})
    client._validate(message, direction='outgoing', api_name='api', procedure_name='proc')


def test_validate_incoming_disabled(create_bus_client_with_unhappy_schema):
    client: BusClient = create_bus_client_with_unhappy_schema(validate={'incoming': False})

    message = RpcMessage(api_name='api', procedure_name='proc', kwargs={'p': 1})
    client._validate(message, direction='incoming', api_name='api', procedure_name='proc')


def test_validate_incoming_enabled(create_bus_client_with_unhappy_schema):
    client: BusClient = create_bus_client_with_unhappy_schema(validate={'incoming': True})

    message = RpcMessage(api_name='api', procedure_name='proc', kwargs={'p': 1})
    with pytest.raises(jsonschema.ValidationError):
        client._validate(message, direction='incoming', api_name='api', procedure_name='proc')


def test_validate_outgoing_disabled(create_bus_client_with_unhappy_schema):
    client: BusClient = create_bus_client_with_unhappy_schema(validate={'outgoing': False})

    message = RpcMessage(api_name='api', procedure_name='proc', kwargs={'p': 1})
    client._validate(message, direction='outgoing', api_name='api', procedure_name='proc')


def test_validate_outgoing_enabled(create_bus_client_with_unhappy_schema):
    client: BusClient = create_bus_client_with_unhappy_schema(validate={'outgoing': True})

    message = RpcMessage(api_name='api', procedure_name='proc', kwargs={'p': 1})
    with pytest.raises(jsonschema.ValidationError):
        client._validate(message, direction='outgoing', api_name='api', procedure_name='proc')
