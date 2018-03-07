import pytest

from lightbus import Event, Api, Parameter
from lightbus.exceptions import InvalidApiForSchemaCreation
from lightbus.schema.schema import api_to_schema


pytestmark = pytest.mark.unit


# api_to_schema()


def test_api_to_schema_event_long_form():
    class TestApi(Api):
        my_event = Event([Parameter('field', bool)])

        class Meta:
            name = 'my.test_api'

    schema = api_to_schema(TestApi())
    assert schema['events']['my_event'] == {
        'parameters': {
            '$schema': 'http://json-schema.org/draft-04/schema#',
            'type': 'object',
            'additionalProperties': False,
            'properties': {'field': {'type': 'boolean'}},
            'required': ['field'],
            'title': 'Event my.test_api.my_event parameters',
        }
    }


def test_api_to_schema_event_short_form():
    class TestApi(Api):
        my_event = Event(['field'])

        class Meta:
            name = 'my.test_api'

    schema = api_to_schema(TestApi())
    assert schema['events']['my_event'] == {
        'parameters': {
            '$schema': 'http://json-schema.org/draft-04/schema#',
            'type': 'object',
            'additionalProperties': False,
            'properties': {'field': {}},
            'required': ['field'],
            'title': 'Event my.test_api.my_event parameters',
        }
    }


def test_api_to_schema_event_private():
    """Properties starting with an underscore should be ignored"""
    class TestApi(Api):
        _my_event = Event(['field'])

        class Meta:
            name = 'my.test_api'

    schema = api_to_schema(TestApi())
    assert not schema['events']


def test_api_to_schema_rpc():
    class TestApi(Api):

        def my_proc(self, field: bool=True) -> str:
            pass

        class Meta:
            name = 'my.test_api'

    schema = api_to_schema(TestApi())
    assert schema['rpcs']['my_proc'] == {
        'parameters': {
            '$schema': 'http://json-schema.org/draft-04/schema#',
            'type': 'object',
            'additionalProperties': False,
            'properties': {'field': {'type': 'boolean', 'default': True}},
            'required': [],
            'title': 'RPC my.test_api.my_proc() parameters',
        },
        'response': {
            '$schema': 'http://json-schema.org/draft-04/schema#',
            'title': 'RPC my.test_api.my_proc() response',
            'type': 'string'
        }
    }


def test_api_to_schema_rpc_private():
    """Methods starting with an underscore should be ignored"""
    class TestApi(Api):

        def _my_proc(self, field: bool=True) -> str:
            pass

        class Meta:
            name = 'my.test_api'

    schema = api_to_schema(TestApi())
    assert not schema['rpcs']


def test_api_to_schema_class_not_instance():
    class TestApi(Api):
        class Meta:
            name = 'my.test_api'

    with pytest.raises(InvalidApiForSchemaCreation):
        api_to_schema(TestApi)
