import inspect
import itertools
import logging
from decimal import Decimal
from typing import Union, Any, Tuple, Sequence, Mapping, Callable, TYPE_CHECKING, get_type_hints

import datetime
from enum import Enum
from uuid import UUID

from lightbus.utilities.deforming import deform_to_bus
from lightbus.utilities.type_checks import parse_hint, issubclass_safe, get_property_default

if TYPE_CHECKING:
    # pylint: disable=unused-import,cyclic-import
    from lightbus.api import Event

NoneType = type(None)
empty = inspect.Parameter.empty
logger = logging.getLogger(__name__)


SCHEMA_URI = "http://json-schema.org/draft-07/schema#"


def make_rpc_parameter_schema(api_name, method_name, method):
    """Create a full parameter JSON schema for the given RPC"""
    parameters = inspect.signature(method).parameters.values()
    schema = make_parameter_schema(parameters)
    schema["title"] = "RPC {}.{}() parameters".format(api_name, method_name)
    return schema


def make_response_schema(api_name: str, method_name: str, method: Callable):
    """Create a full response JSON schema for the given RPC"""
    sig = inspect.signature(method)
    schema = return_type_to_schema(sig.return_annotation)
    schema["title"] = "RPC {}.{}() response".format(api_name, method_name)
    schema["$schema"] = SCHEMA_URI
    return schema


def make_event_parameter_schema(api_name, method_name, event: "Event"):
    """Create a full parameter JSON schema for the given event"""
    parameters = _normalise_event_parameters(event.parameters)
    schema = make_parameter_schema(parameters)
    schema["title"] = "Event {}.{} parameters".format(api_name, method_name)
    return schema


def make_parameter_schema(parameters: Sequence[inspect.Parameter]):
    """Create a full JSON schema for the given parameters"""
    parameter_schema = {
        "$schema": SCHEMA_URI,
        "type": "object",
        "additionalProperties": False,
        "properties": {},
        "required": [],
    }

    for parameter in parameters:
        if parameter.kind in (parameter.POSITIONAL_ONLY, parameter.VAR_POSITIONAL):
            logger.warning(
                "Positional-only arguments are not supported in event or RPC parameters: {}".format(
                    parameter
                )
            )
            continue

        if parameter.kind == parameter.VAR_KEYWORD:
            parameter_schema["additionalProperties"] = True
            continue

        parameter_schema["properties"][parameter.name] = parameter_to_schema(parameter)
        if parameter.default is empty:
            parameter_schema["required"].append(parameter.name)

    # required key should not be present if it is empty
    if not parameter_schema["required"]:
        parameter_schema.pop("required")

    return parameter_schema


def parameter_to_schema(parameter):
    """Convert an `inspect.Parameter` to a single JSON schema

    This will create the schemas using `annotation_to_json_schemas()`,
    set the default values based on the parameters default value
    annotation, and combine the schemas with `wrap_with_any_of()`
    """
    type_schemas = annotation_to_json_schemas(parameter.annotation, parameter.default)
    if not type_schemas:
        return {}

    schemas = []
    for type_schema in type_schemas:
        if parameter.default is not empty:
            type_schema["default"] = parameter.default
        schemas.append(type_schema)

    return wrap_with_any_of(schemas)


def return_type_to_schema(type_):
    """Convert the return type annotation into a json schema

    Mostly a wrapper around `python_type_to_json_schemas()`.
    """
    if type_ is empty:
        return {}

    type_schemas = python_type_to_json_schemas(type_)
    if not type_schemas:
        return {}

    return wrap_with_any_of(type_schemas)


def annotation_to_json_schemas(annotation, default=empty):
    """Convert an `inspect.Parameter` to JSON schemas

    This wraps `python_type_to_json_schemas()` with a little extra logic. Specifically,
    it determines the python type from:

        1. The parameter's annotation, falling back to...
        2. The type of the parameters default value

    This will return a list of JSON schemas. See note one `python_type_to_json_schemas()`
    """
    if annotation is not empty:
        schemas = python_type_to_json_schemas(annotation)

        # Allow nulls if the parameter has a default value of None
        if default is not empty and default is None:
            null_schema = {"type": "null"}
            if null_schema not in schemas:
                schemas.append(null_schema)

    elif default is not empty and default is not None:
        schemas = python_type_to_json_schemas(type(default))
    else:
        schemas = [{}]

    return schemas


def python_type_to_json_schemas(type_):
    """Convert a python type hint to its JSON schema representations

    Note that a type hint may actually have several possible representations,
    which is why this function returns a list of schemas. An example of this is
    the `Union` type hint. These are later combined via `wrap_with_any_of()`
    """
    # pylint: disable=too-many-return-statements
    type_, hint_args = parse_hint(type_)
    if type_ == Union:
        return list(itertools.chain(*map(python_type_to_json_schemas, hint_args)))
    if type_ == empty:
        return [{}]
    elif type_ in (Any, ...):
        return [{}]
    elif hasattr(type_, "__to_bus__"):
        return python_type_to_json_schemas(inspect.signature(type_.__to_bus__).return_annotation)
    elif issubclass_safe(type_, (str, bytes, complex, UUID)):
        return [{"type": "string"}]
    elif issubclass_safe(type_, Decimal):
        return [{"type": "string", "pattern": r"^-?\d+(\.\d+)?$"}]
    elif issubclass_safe(type_, (bool,)):
        return [{"type": "boolean"}]
    elif issubclass_safe(type_, (float,)):
        return [{"type": "number"}]
    elif issubclass_safe(type_, (int,)):
        return [{"type": "integer"}]
    elif issubclass_safe(type_, (Mapping,)) and hint_args and hint_args[0] == str:
        # Mapping with strings as keys
        return [
            {
                "type": "object",
                "additionalProperties": wrap_with_any_of(python_type_to_json_schemas(hint_args[1])),
            }
        ]
    elif issubclass_safe(type_, (dict, Mapping)):
        return [{"type": "object"}]
    elif issubclass_safe(type_, tuple) and hasattr(type_, "_fields"):
        # Named tuple
        return [make_custom_object_schema(type_, property_names=type_._fields)]
    elif issubclass_safe(type_, Enum) and type_.__members__:
        # Enum
        enum_first_value = list(type_.__members__.values())[0].value
        schema = {}
        try:
            schema["type"] = python_type_to_json_schemas(type(enum_first_value))[0]["type"]
            schema["enum"] = [v.value for v in type_.__members__.values()]
        except KeyError:
            logger.warning(f"Could not determine type for values in enum: {type_}")
        return [schema]
    elif issubclass_safe(type_, (Tuple,)) and hint_args:
        return [
            {
                "type": "array",
                "maxItems": len(hint_args),
                "minItems": len(hint_args),
                "prefixItems": [
                    wrap_with_any_of(python_type_to_json_schemas(sub_type))
                    for sub_type in hint_args
                ],
            }
        ]
    elif issubclass_safe(type_, (list, tuple, set)):
        schema = {"type": "array"}
        if hint_args:
            schema["items"] = wrap_with_any_of(python_type_to_json_schemas(hint_args[0]))
        return [schema]
    elif issubclass_safe(type_, NoneType) or type_ is None:
        return [{"type": "null"}]
    elif issubclass_safe(type_, (datetime.datetime)):
        return [{"type": "string", "format": "date-time"}]
    elif issubclass_safe(type_, (datetime.date)):
        return [{"type": "string", "format": "date"}]
    elif issubclass_safe(type_, (datetime.time)):
        return [{"type": "string", "format": "time"}]
    elif getattr(type_, "__annotations__", None):
        # Custom class
        return [make_custom_object_schema(type_)]
    else:
        logger.warning(
            f"Could not convert python type to json schema type: {type_}. If it is a class, "
            "ensure it's class-level variables have type hints."
        )
        return [{}]


def make_custom_object_schema(type_, property_names=None):
    """Convert a named tuple into a JSON schema

    While a lot of conversations happen in `python_type_to_json_schemas()`, conversion of
    annotated classes is a little more involved. It therefore gets its own function.

    While this is mainly used for named tuples within lightbus, it can also be used for
    general classes.
    """
    if property_names is None:
        # Use typing.get_type_hints() rather than `__annotations__`, as this will resolve
        # forward references
        property_names = [
            p for p in set(list(get_type_hints(type_).keys()) + dir(type_)) if p[0] != "_"
        ]

    properties = {}
    required = []
    for property_name in property_names:
        property_value = getattr(type_, property_name, None)
        if callable(property_value):
            # is a method or dynamic property
            continue

        default = get_property_default(type_, property_name)
        type_hints = get_type_hints(type_)
        if hasattr(type_, "__annotations__") and property_name in type_hints:
            # Use typing.get_type_hints() rather than `__annotations__`, as this will resolve
            # forward references
            properties[property_name] = wrap_with_any_of(
                annotation_to_json_schemas(annotation=type_hints[property_name], default=default)
            )
        elif default is not empty:
            properties[property_name] = wrap_with_any_of(python_type_to_json_schemas(type(default)))
        else:
            properties[property_name] = {}

        if default is empty:
            required.append(property_name)
        else:
            properties[property_name]["default"] = deform_to_bus(default)

    schema = {
        "type": "object",
        "title": type_.__name__,
        "properties": properties,
        "required": required,
        "additionalProperties": False,
    }

    # required key should not be present if it is empty
    if not schema["required"]:
        schema.pop("required")

    return schema


def wrap_with_any_of(schemas: Sequence):
    """Take multiple JSON schemas and combine them using the `anyOf` JSON schema notation"""
    if len(schemas) == 1:
        return schemas[0]
    else:
        return {"anyOf": schemas}


def _normalise_event_parameters(parameters: Sequence) -> Sequence:
    """Ensure the given event parameters are in the long format

    Event parameters can be specified as strings or `inspect.Parameter`
    instances. Ensure all provided parameters are all in the latter form.
    """
    # pylint: disable=cyclic-import,import-outside-toplevel
    from lightbus.schema import Parameter

    normalised = []
    for parameter in parameters:
        if isinstance(parameter, str):
            normalised.append(Parameter(name=parameter))
        else:
            normalised.append(parameter)
    return normalised
