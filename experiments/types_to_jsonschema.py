"""Can we generate a json schema from python 3 type hints?

Run the tests/examples using::

    py.test experiments/types_to_jsonschema.py

"""
import inspect
import json

from decimal import Decimal

from typing import Union, Optional, Tuple

import itertools

import logging

EMPTY = inspect.Signature.empty
NoneType = type(None)


def check_password(username: str, password: Union[str, int], new_session: Optional[bool]) -> bool:
    return True


def python_type_to_json_types(type_):
    if type(type_) == type(Union):
        sub_types = type_._subs_tree()[1:]
        return list(itertools.chain(*map(python_type_to_json_types, sub_types)))
    elif issubclass(type_, (str, bytes, Decimal, complex)):
        return ['string']
    elif issubclass(type_, (bool, )):
        return ['boolean']
    elif issubclass(type_, (int, float)):
        return ['number']
    elif issubclass(type_, (dict, )):
        return ['object']
    elif issubclass(type_, (list, tuple)):
        import pdb; pdb.set_trace()
        return ['array']
    elif issubclass(type_, NoneType):
        return ['null']
    else:
        return ['string']


def parameter_to_json_types(parameter):
    if parameter.annotation is not EMPTY:
        return python_type_to_json_types(parameter.annotation)
    elif parameter.default is not EMPTY:
        return python_type_to_json_types(type(parameter.default))
    else:
        return None


def parameter_to_schema(parameter):
    types = parameter_to_json_types(parameter)
    if not types:
        return {}

    schemas = []
    for type_ in types:
        s = {}
        if type_ is not None:
            s['type'] = type_
        if parameter.default is not EMPTY:
            s['default'] = parameter.default
        schemas.append(s)

    if len(schemas) == 1:
        return schemas[0]
    else:
        return {'oneOf': schemas}


def return_type_to_schema(type_):
    if type_ is EMPTY:
        return {}

    types = python_type_to_json_types(type_)
    if not types:
        return {}

    json_types = python_type_to_json_types(type_)
    schemas = [{'type': json_type for json_type in json_types}]

    if len(schemas) == 1:
        return schemas[0]
    else:
        return {'oneOf': schemas}


def make_parameter_schema(f):
    parameter_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'description': '{}() parameters'.format(f.__name__),
        'type': 'object',
        'properties': {},
        'required': [],
    }
    sig = inspect.signature(f)
    for parameter in sig.parameters.values():
        parameter_schema['properties'][parameter.name] = parameter_to_schema(parameter)
        if parameter.default is EMPTY:
            parameter_schema['required'].append(parameter.name)

    logging.info(json.dumps(parameter_schema, indent=4))
    return parameter_schema


def make_response_schema(f):
    response_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'description': '{}() parameters'.format(f.__name__),
    }
    sig = inspect.signature(f)
    response_schema.update(
        **return_type_to_schema(sig.return_annotation)
    )
    logging.info(json.dumps(response_schema, indent=4))
    return response_schema


def test_no_types():
    def func(username): pass
    schema = make_parameter_schema(func)
    assert schema['description'] == 'func() parameters'
    assert schema['properties']['username'] == {}
    assert schema['required'] == ['username']


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


def test_python_type_to_json_types_union():
    json_types = python_type_to_json_types(Union[str, int])
    assert json_types == ['string', 'number']


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

