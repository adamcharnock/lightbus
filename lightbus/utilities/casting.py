# The opposite of deforming. See lightbus.utilities.deforming
import datetime
import inspect
import logging
from enum import Enum
from typing import Mapping, Type, get_type_hints, Union, TypeVar, Callable, Any
from base64 import b64decode

from lightbus.utilities.type_checks import (
    type_is_namedtuple,
    type_is_dataclass,
    is_optional,
    isinstance_safe,
    parse_hint,
    issubclass_safe,
)

is_callable = callable
logger = logging.getLogger(__name__)


def cast_to_signature(parameters: dict, callable) -> dict:
    """Cast parameters into the type hints provided by callable's function signature.

    Will resolve functions which have been wrapped using itertools.wrap()
    """
    casted_parameters = parameters.copy()
    for key, hint in get_type_hints(callable).items():
        if key not in casted_parameters:
            continue

        casted_parameters[key] = cast_to_hint(value=casted_parameters[key], hint=hint)

    return casted_parameters


V = TypeVar("V")
H = TypeVar("A")


def cast_to_hint(value: V, hint: H) -> Union[V, H]:
    """Cast a value into a given type hint

    If a value cannot be cast then the original value will be returned
    and a warning emitted
    """
    # pylint: disable=too-many-return-statements
    if value is None:
        return None
    elif hint in (Any, ...):
        return value

    optional_hint = is_optional(hint)
    if optional_hint and value is not None:
        hint = optional_hint

    hint_type, hint_args = parse_hint(hint)
    is_class = inspect.isclass(hint_type)

    if hint_type == Union:
        # We don't attempt to deal with unions for now
        return value
    elif hint == inspect.Parameter.empty:
        # Empty annotation
        return value
    elif isinstance_safe(value, hint):
        # Already correct type
        return value
    elif hasattr(hint, "__from_bus__"):
        # Hint supports custom deserializing.
        return _mapping_to_instance(
            mapping=value,
            destination_type=hint,
            instantiator=hint.__from_bus__,
            expand_kwargs=False,
        )
    elif issubclass_safe(hint, bytes):
        return b64decode(value.encode("utf8"))
    elif type_is_namedtuple(hint) and isinstance_safe(value, Mapping):
        return _mapping_to_instance(mapping=value, destination_type=hint)
    elif type_is_dataclass(hint) and isinstance_safe(value, Mapping):
        return _mapping_to_instance(mapping=value, destination_type=hint)
    elif is_class and issubclass_safe(hint_type, datetime.datetime) and isinstance_safe(value, str):
        # Datetime as a string
        return datetime.datetime.fromisoformat(value)
    elif is_class and issubclass_safe(hint_type, datetime.date) and isinstance_safe(value, str):
        # Date as a string
        return cast_or_warning(lambda v: datetime.datetime.fromisoformat(v).date(), value)
    elif is_class and issubclass_safe(hint_type, list):
        # Lists
        if hint_args and hasattr(value, "__iter__"):
            value = [cast_to_hint(i, hint_args[0]) for i in value]
        return cast_or_warning(list, value)
    elif is_class and issubclass_safe(hint_type, tuple):
        # Tuples
        if hint_args and hasattr(value, "__iter__"):
            value = [cast_to_hint(h, hint_args[i]) for i, h in enumerate(value)]
        return cast_or_warning(tuple, value)
    elif is_class and issubclass_safe(hint_type, set):
        # Sets
        if hint_args and hasattr(value, "__iter__"):
            value = [cast_to_hint(i, hint_args[0]) for i in value]
        return cast_or_warning(set, value)
    elif (
        inspect.isclass(hint)
        and hasattr(hint, "__annotations__")
        and not issubclass_safe(hint_type, Enum)
    ):
        logger.warning(
            f"Cannot cast to arbitrary class {hint}, using un-casted value. "
            f"If you want to receive custom objects you can 1) "
            f"use a NamedTuple, 2) use a dataclass, or 3) specify the "
            f"__from_bus__() and __to_bus__() magic methods."
        )
        return value
    else:
        return cast_or_warning(hint, value)


T = TypeVar("T")


def _mapping_to_instance(
    mapping: Mapping, destination_type: Type[T], instantiator: Callable = None, expand_kwargs=True
) -> T:
    """Convert a dictionary-like object into an instance of the given type

    This conversion is performed recursively. If the passed type
    class contains child structures, then the corresponding
    child keys of the dictionary will be mapped.

    If `instantiator` is provided, it will be called rather than simply
    calling `destination_type`.

    If `expand_kwargs` is True then the the parameters will be
    expended (i.e. **) when the `destination_type` / `instantiator`
    is called.
    """
    import lightbus.config.structure

    hints = get_type_hints(destination_type, None, lightbus.config.structure.__dict__)
    parameters = {}

    if mapping is None:
        return None

    # Iterate through each key/type-hint pairing in the destination type
    for key, hint in hints.items():
        value = mapping.get(key)

        if key not in mapping:
            # This attribute has not been provided by in the mapping. Skip it
            continue

        hint_type, hint_args = parse_hint(hint)

        # Is this an Optional[] hint (which looks like Union[Thing, None])
        if is_optional(hint) and value is not None:
            # Yep, it's an Optional. So unwrap it.
            hint = hint_args[0]

        if issubclass_safe(hint_type, Mapping) and hint_args and len(hint_args) == 2:
            parameters[key] = dict()
            for k, v in value.items():
                parameters[key][k] = cast_to_hint(v, hint_args[1])
        else:
            parameters[key] = cast_to_hint(value, hint)

    instantiator = instantiator or destination_type
    if expand_kwargs:
        return instantiator(**parameters)
    else:
        return instantiator(parameters)


def cast_or_warning(type_, value):
    try:
        return type_(value)
    except Exception as e:
        logger.warning(
            f"Failed to cast value {repr(value)} to type {type_}. Will "
            f"continue without casting, but this may cause errors in any "
            f"called code. Error was: {e}"
        )
        return value
