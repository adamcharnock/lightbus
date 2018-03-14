import inspect
import itertools
import json
import logging
from _pydecimal import Decimal
from typing import Union, Any, Tuple, Sequence, Mapping, Callable

import lightbus

NoneType = type(None)
empty = inspect.Signature.empty
logger = logging.getLogger(__name__)


SCHEMA_URI = 'http://json-schema.org/draft-04/schema#'


def make_rpc_parameter_schema(api_name, method_name, method):
    """Create a full parameter JSON schema for the given RPC
    """
    parameters = inspect.signature(method).parameters.values()
    schema = make_parameter_schema(parameters)
    schema['title'] = 'RPC {}.{}() parameters'.format(api_name, method_name)
    return schema


def make_response_schema(api_name: str, method_name: str, method: Callable):
    """Create a full response JSON schema for the given RPC
    """
    sig = inspect.signature(method)
    schema = return_type_to_schema(sig.return_annotation)
    schema['title'] = 'RPC {}.{}() response'.format(api_name, method_name)
    schema['$schema'] = SCHEMA_URI
    return schema


def make_event_parameter_schema(api_name, method_name, event: 'lightbus.Event'):
    """Create a full parameter JSON schema for the given event
    """
    parameters = _normalise_event_parameters(event.parameters)
    schema = make_parameter_schema(parameters)
    schema['title'] = 'Event {}.{} parameters'.format(api_name, method_name)
    return schema


def make_parameter_schema(parameters: Sequence[inspect.Parameter]):
    """Create a full JSON schema for the given parameters
    """
    parameter_schema = {
        '$schema': SCHEMA_URI,
        'type': 'object',
        'additionalProperties': False,
        'properties': {},
        'required': [],
    }

    for parameter in parameters:
        if parameter.kind in (parameter.POSITIONAL_ONLY, parameter.VAR_POSITIONAL):
            logger.warning('Positional-only arguments are not supported in event or RPC parameters: {}'.format(parameter))
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


def parameter_to_schema(parameter):
    """Convert an `inspect.Parameter` to a single JSON schema

    This will create the schemas using `parameter_to_json_schemas()`,
    set the default values based on the parameters default value
    annotation, and combine the schemas with `wrap_with_one_of()`
    """
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
    """Convert the return type annotation into a json schema

    Mostly a wrapper around `python_type_to_json_schemas()`.
    """
    if type_ is empty:
        return {}

    type_schemas = python_type_to_json_schemas(type_)
    if not type_schemas:
        return {}

    return wrap_with_one_of(type_schemas)


def parameter_to_json_schemas(parameter):
    """Convert an `inspect.Parameter` to JSON schemas

    This wraps `python_type_to_json_schemas()` with a little extra logic. Specifically,
    it determines the python type from:

        1. The parameter's annotation, falling back to...
        2. The type of the parameters default value

    This will return a list of JSON schemas. See note one `python_type_to_json_schemas()`
    """
    if parameter.annotation is not empty:
        return python_type_to_json_schemas(parameter.annotation)
    elif parameter.default is not empty:
        return python_type_to_json_schemas(type(parameter.default))
    else:
        return None


def python_type_to_json_schemas(type_):
    """Convert a python type hint to its JSON schema representations

    Note that a type hint may actually have several possible representations,
    which is why this function returns a list of schemas. An example of this is
    the `Union` type hint. These are later combined via `wrap_with_one_of()`
    """
    is_class = inspect.isclass(type_)

    if hasattr(type_, '_subs_tree') and isinstance(type_._subs_tree(), Sequence):
        subs_tree = type_._subs_tree()
    else:
        subs_tree = None

    if type(type_) == type(Union):
        sub_types = subs_tree[1:]
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
    elif is_class and issubclass(type_, (Mapping, )) and subs_tree and subs_tree[1] == str:
        # Mapping with strings as keys
        return [{
            'type': 'object',
            'patternProperties': {'.*': wrap_with_one_of(python_type_to_json_schemas(subs_tree[2]))}
        }]
    elif is_class and issubclass(type_, tuple) and hasattr(type_, '_fields'):
        # Named tuple
        return [make_custom_object_schema(type_, property_names=type_._fields)]
    elif type(type_) == type(Tuple) and len(subs_tree) > 1:
        sub_types = subs_tree[1:]
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


def make_custom_object_schema(type_, property_names=None):
    """Convert a named tuple into a JSON schema

    While a lot of conversations happen in `python_type_to_json_schemas()`, conversion of
    named tuples is a little more involved. It therefore gets its own function.

    While this is mainly used for named tuples within lightbus, it can also be used for
    general classes.
    """
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


def wrap_with_one_of(schemas: Sequence):
    """Take multiple JSON schemas and combine them using the `oneOf` JSON schema notation"""
    if len(schemas) == 1:
        return schemas[0]
    else:
        return {'oneOf': schemas}


def _normalise_event_parameters(parameters: Sequence) -> Sequence:
    """Ensure the given event parameters are in the long format

    Event parameters can be specified as strings or `inspect.Parameter`
    instances. Ensure all provided parameters are all in the latter form.
    """
    from lightbus.schema import Parameter

    normalised = []
    for parameter in parameters:
        if isinstance(parameter, str):
            normalised.append(Parameter(name=parameter))
        else:
            normalised.append(parameter)
    return normalised
