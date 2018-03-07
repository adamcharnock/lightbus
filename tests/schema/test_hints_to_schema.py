from typing import Union, Optional, NamedTuple, Tuple, Any
from collections import namedtuple

import pytest

from lightbus.schema.hints_to_schema import make_parameter_schema, python_type_to_json_schemas, make_response_schema

pytestmark = pytest.mark.unit


def test_no_types():
    def func(username): pass
    schema = make_parameter_schema(func)
    assert schema['description'] == 'func() parameters'
    assert schema['properties']['username'] == {}
    assert schema['required'] == ['username']
    assert schema['additionalProperties'] is False


def test_default():
    def func(field=123): pass
    schema = make_parameter_schema(func)
    assert schema['description'] == 'func() parameters'
    assert schema['properties']['field'] == {'type': 'number', 'default': 123}
    assert schema['required'] == []


def test_type():
    def func(field: dict): pass
    schema = make_parameter_schema(func)
    assert schema['description'] == 'func() parameters'
    assert schema['properties']['field'] == {'type': 'object'}
    assert schema['required'] == ['field']


def test_type_with_default():
    def func(field: float=3.142): pass
    schema = make_parameter_schema(func)
    assert schema['description'] == 'func() parameters'
    assert schema['properties']['field'] == {'type': 'number', 'default': 3.142}
    assert schema['required'] == []


def test_kwargs():
    def func(field: dict, **kwargs): pass
    schema = make_parameter_schema(func)
    assert schema['description'] == 'func() parameters'
    # **kwargs isn't a property, but additionalProperties is now set to true
    assert list(schema['properties'].keys()) == ['field']
    assert schema['required'] == ['field']
    assert schema['additionalProperties'] is True


def test_positional_args():
    def func(field: dict, *args): pass
    schema = make_parameter_schema(func)
    assert schema['description'] == 'func() parameters'
    assert list(schema['properties'].keys()) == ['field']  # *args is ignored
    assert schema['required'] == ['field']
    assert schema['additionalProperties'] is False


def test_python_type_to_json_types_union():
    json_types = python_type_to_json_schemas(Union[str, int])
    assert json_types == [
        {'type': 'string'},
        {'type': 'number'},
    ]


def test_union():
    def func(field: Union[str, int]): pass
    schema = make_parameter_schema(func)
    assert schema['description'] == 'func() parameters'
    assert schema['properties']['field'] == {
        'oneOf': [
            {'type': 'string'},
            {'type': 'number'},
        ]
    }
    assert schema['required'] == ['field']


def test_union_default():
    def func(field: Union[str, int] = 123): pass
    schema = make_parameter_schema(func)
    assert schema['description'] == 'func() parameters'
    assert schema['properties']['field'] == {
        'oneOf': [
            {'type': 'string', 'default': 123},  # Technically an invalid default value
            {'type': 'number', 'default': 123},
        ]
    }
    assert schema['required'] == []


def test_optional():
    def func(username: Optional[str]): pass
    schema = make_parameter_schema(func)
    assert schema['description'] == 'func() parameters'
    assert schema['properties']['username'] == {
        'oneOf': [
            {'type': 'string'},
            {'type': 'null'},
        ]
    }
    assert schema['required'] == ['username']


def test_named_tuple():

    class User(NamedTuple):
        username: str
        password: str
        is_admin: bool = False

    def func(user: User): pass
    schema = make_parameter_schema(func)

    assert schema['properties']['user']['title'] == 'User'
    assert schema['properties']['user']['type'] == 'object'
    assert schema['properties']['user']['properties'] == {
            'username': {'type': 'string'},
            'password': {'type': 'string'},
            'is_admin': {'type': 'boolean', 'default': False},
        }
    assert set(schema['properties']['user']['required']) == {'password', 'username'}


def test_named_tuple_using_function():

    User = namedtuple('User', ('username', 'password'))

    def func(user: User): pass
    schema = make_parameter_schema(func)

    assert schema['properties']['user']['title'] == 'User'
    assert schema['properties']['user']['type'] == 'object'
    assert schema['properties']['user']['properties'] == {
            'username': {},
            'password': {},
        }
    assert set(schema['properties']['user']['required']) == {'password', 'username'}


def test_response_no_types():
    def func(username): pass
    schema = make_response_schema(func)
    assert 'type' not in schema


def test_response_bool():
    def func(username) -> bool: pass
    schema = make_response_schema(func)
    assert schema['type'] == 'boolean'


def test_response_typed_tuple():
    def func(username) -> Tuple[str, int, bool]: pass
    schema = make_response_schema(func)
    assert schema['type'] == 'array'
    assert schema['items'] == [
        {'type': 'string'},
        {'type': 'number'},
        {'type': 'boolean'},
    ]


def test_response_named_tuple():

    class User(NamedTuple):
        username: str
        password: str
        is_admin: bool = False

    def func(username) -> User: pass
    schema = make_response_schema(func)
    assert schema['type'] == 'object'
    assert schema['title'] == 'User'
    assert schema['properties'] == {
        'username': {'type': 'string'},
        'password': {'type': 'string'},
        'is_admin': {'type': 'boolean', 'default': False},
    }
    assert set(schema['required']) == {'username', 'password'}
    assert schema['additionalProperties'] == False


def test_unknown_type():
    class UnknownThing(object): pass

    def func(username) -> UnknownThing: pass
    schema = make_response_schema(func)
    assert 'type' not in schema


def test_any():
    def func(username) -> Any: pass
    schema = make_response_schema(func)
    assert 'type' not in schema


def test_ellipsis():
    def func(username) -> ...: pass
    schema = make_response_schema(func)
    assert 'type' not in schema
