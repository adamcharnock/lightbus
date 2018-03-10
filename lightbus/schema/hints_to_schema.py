import inspect
import itertools
import json
import logging
from _pydecimal import Decimal
from typing import Union, Any, Tuple, Sequence

import lightbus

NoneType = type(None)
empty = inspect.Signature.empty
logger = logging.getLogger(__name__)


SCHEMA_URI = 'http://json-schema.org/draft-04/schema#'


def wrap_with_one_of(schemas):
    if len(schemas) == 1:
        return schemas[0]
    else:
        return {'oneOf': schemas}


def make_custom_object_schema(type_, property_names=None):
    if property_names is None:
        property_names = [p for p in set(list(type_.__annotations__.keys()) + dir(type_)) if p[0] != '_']

    properties = {}
    required = []
    for property_name in property_names:
        default = empty

        if issubclass(type_, tuple):
            # namedtuple
            if hasattr(type_, '_field_defaults'):
                default = type_._field_defaults.get(property_name, empty)
        else:
            default = getattr(type_, property_name, empty)

        if callable(default):
            default = empty

        if hasattr(type_, '__annotations__'):
            properties[property_name] = wrap_with_one_of(
                python_type_to_json_schemas(
                    type_.__annotations__.get(property_name, None)
                )
            )
        elif default is not empty:
            properties[property_name] = wrap_with_one_of(python_type_to_json_schemas(type(default)))
        else:
            properties[property_name] = {}

        if default is empty:
            required.append(property_name)
        else:
            properties[property_name]['default'] = default

    schema = {
        'type': 'object',
        'title': type_.__name__,
        'properties': properties,
        'required': required,
        'additionalProperties': False,
    }

    # required key should not be present if it is empty
    if not schema['required']:
        schema.pop('required')

    return schema


def python_type_to_json_schemas(type_):
    is_class = inspect.isclass(type_)
    if type(type_) == type(Union):
        sub_types = type_._subs_tree()[1:]
        return list(itertools.chain(*map(python_type_to_json_schemas, sub_types)))
    elif type_ in (Any, ...):
        return [{}]
    elif is_class and issubclass(type_, (str, bytes, Decimal, complex)):
        return [{'type': 'string'}]
    elif is_class and issubclass(type_, (bool, )):
        return [{'type': 'boolean'}]
    elif is_class and issubclass(type_, (int, float)):
        return [{'type': 'number'}]
    elif is_class and issubclass(type_, (dict, )):
        return [{'type': 'object'}]
    elif is_class and issubclass(type_, tuple) and hasattr(type_, '_fields'):
        # Named tuple
        return [make_custom_object_schema(type_, property_names=type_._fields)]
    elif type(type_) == type(Tuple) and len(type_._subs_tree()) > 1:
        sub_types = type_._subs_tree()[1:]
        return [{
            'type': 'array',
            'maxItems': len(sub_types),
            'minItems': len(sub_types),
            'items': [wrap_with_one_of(python_type_to_json_schemas(sub_type)) for sub_type in sub_types]
        }]
    elif is_class and issubclass(type_, (list, tuple)):
        return [{'type': 'array'}]
    elif is_class and issubclass(type_, NoneType):
        return [{'type': 'null'}]
    else:
        logger.warning('Could not convert python type to json schema type: {}'.format(type_))
        return [{}]


def parameter_to_json_schemas(parameter):
    if parameter.annotation is not empty:
        return python_type_to_json_schemas(parameter.annotation)
    elif parameter.default is not empty:
        return python_type_to_json_schemas(type(parameter.default))
    else:
        return None


def parameter_to_schema(parameter):
    type_schemas = parameter_to_json_schemas(parameter)
    if not type_schemas:
        return {}

    schemas = []
    for type_schema in type_schemas:
        if parameter.default is not empty:
            type_schema['default'] = parameter.default
        schemas.append(type_schema)

    return wrap_with_one_of(schemas)


def return_type_to_schema(type_):
    if type_ is empty:
        return {}

    type_schemas = python_type_to_json_schemas(type_)
    if not type_schemas:
        return {}

    return wrap_with_one_of(type_schemas)


def make_parameter_schema(parameters: Sequence[inspect.Parameter]):
    parameter_schema = {
        '$schema': SCHEMA_URI,
        'type': 'object',
        'additionalProperties': False,
        'properties': {},
        'required': [],
    }

    for parameter in parameters:
        if parameter.kind in (parameter.POSITIONAL_ONLY, parameter.VAR_POSITIONAL):
            logger.warning('Positional-only arguments are not supported: {}'.format(parameter))
            continue

        if parameter.kind == parameter.VAR_KEYWORD:
            parameter_schema['additionalProperties'] = True
            continue

        parameter_schema['properties'][parameter.name] = parameter_to_schema(parameter)
        if parameter.default is empty:
            parameter_schema['required'].append(parameter.name)

    # required key should not be present if it is empty
    if not parameter_schema['required']:
        parameter_schema.pop('required')

    return parameter_schema


def make_rpc_parameter_schema(api_name, method_name, method):
    # Get the method parameters by inspecting the method's signature
    parameters = inspect.signature(method).parameters.values()
    schema = make_parameter_schema(parameters)
    schema['title'] = 'RPC {}.{}() parameters'.format(api_name, method_name)
    return schema


def make_event_parameter_schema(api_name, method_name, event: 'lightbus.Event'):
    # Get the event parameters via whatever was declared for the event
    parameters = _normalise_event_parameters(event.parameters)
    schema = make_parameter_schema(parameters)
    schema['title'] = 'Event {}.{} parameters'.format(api_name, method_name)
    return schema


def make_response_schema(api_name, method_name, method):
    sig = inspect.signature(method)
    schema = return_type_to_schema(sig.return_annotation)
    schema['title'] = 'RPC {}.{}() response'.format(api_name, method_name)
    schema['$schema'] = SCHEMA_URI
    return schema


def _normalise_event_parameters(parameters):
    from lightbus.schema import Parameter

    normalised = []
    for parameter in parameters:
        if isinstance(parameter, str):
            normalised.append(Parameter(name=parameter))
        else:
            normalised.append(parameter)
    return normalised
