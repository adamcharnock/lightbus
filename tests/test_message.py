import jsonschema
import pytest
from jsonschema import ValidationError

from lightbus import RpcMessage, Schema, ResultMessage, EventMessage

pytestmark = pytest.mark.unit


@pytest.yield_fixture()
def unhappy_schema(mocker):
    """Schema which always fails to validate"""
    schema = Schema(schema_transport=None)

    fake_schema = {'parameters': {'p': {}}, 'response': {}}
    mocker.patch.object(schema, 'get_rpc_schema', autospec=True, return_value=fake_schema),
    mocker.patch.object(schema, 'get_event_schema', autospec=True, return_value=fake_schema),
    mocker.patch('jsonschema.validate', autospec=True, side_effect=ValidationError('test error')),
    return schema


def test_message_rpc_validate(unhappy_schema):
    with pytest.raises(jsonschema.ValidationError):
        RpcMessage(api_name='api', procedure_name='proc', kwargs={'p': 1}).validate(schema=unhappy_schema)
    jsonschema.validate.assert_called_with({'p': 1}, {'p': {}})


def test_message_result_validate(unhappy_schema):
    with pytest.raises(jsonschema.ValidationError):
        ResultMessage(result='123', rpc_id='123').validate(api_name='api', procedure_name='proc', schema=unhappy_schema)
    jsonschema.validate.assert_called_with('123', {})


def test_event_result_validate(unhappy_schema):
    with pytest.raises(jsonschema.ValidationError):
        EventMessage(api_name='api', event_name='proc', kwargs={'p': 1}).validate(schema=unhappy_schema)
    jsonschema.validate.assert_called_with({'p': 1}, {'p': {}})
