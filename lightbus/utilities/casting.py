# The opposite of deforming. See lightbus.utilities.deforming
import datetime
import inspect
import logging
from enum import Enum
from typing import Mapping, Type, get_type_hints, Union, TypeVar

import dateutil.parser

from lightbus.utilities.type_checks import (
    type_is_namedtuple,
    type_is_dataclass,
    is_optional,
    isinstance_safe,
)

is_callable = callable
logger = logging.getLogger(__name__)


def cast_to_signature(parameters: dict, callable) -> dict:
    for key, hint in get_type_hints(callable).items():
        if key not in parameters:
            continue

        parameters[key] = cast_to_hint(value=parameters[key], hint=hint)

    return parameters


V = TypeVar("V")
H = TypeVar("A")


def cast_to_hint(value: V, hint: H) -> Union[V, H]:
    optional_hint = is_optional(hint)
    if optional_hint and value is not None:
        hint = optional_hint

    subs_tree = hint._subs_tree() if hasattr(hint, "_subs_tree") else None
    subs_tree = subs_tree if isinstance(subs_tree, tuple) else None
    is_class = inspect.isclass(hint)

    if type(hint) == type(Union):
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
        return hint.__from_bus__(value)
    elif type_is_namedtuple(hint) and isinstance_safe(value, Mapping):
        return mapping_to_named_tuple(mapping=value, named_tuple=hint)
    elif type_is_dataclass(hint) and isinstance_safe(value, Mapping):
        # We can treat dataclasses the same as named tuples
        return mapping_to_named_tuple(mapping=value, named_tuple=hint)
    elif is_class and issubclass(hint, datetime.datetime) and isinstance_safe(value, str):
        # Datetime as a string
        return dateutil.parser.parse(value)
    elif is_class and issubclass(hint, datetime.date) and isinstance_safe(value, str):
        # Date as a string
        return dateutil.parser.parse(value).date()
    elif is_class and issubclass(hint, list):
        # Lists
        if subs_tree:
            return [cast_to_hint(i, subs_tree[1]) for i in value]
        else:
            return list(value)
    elif is_class and issubclass(hint, tuple):
        # Tuples
        if subs_tree:
            return tuple(cast_to_hint(i, subs_tree[1]) for i in value)
        else:
            return tuple(value)
    elif inspect.isclass(hint) and hasattr(hint, "__annotations__") and not issubclass(hint, Enum):
        logger.warning(
            f"Cannot cast to arbitrary class {hint}, using un-casted value. "
            f"If you want to receive custom objects you can 1) "
            f"use a NamedTuple, 2) use a dataclass, or 3) specify the "
            f"__from_bus__() and __to_bus__() magic methods."
        )
        return value
    else:
        try:
            return hint(value)
        except Exception as e:
            logger.warning(
                f"Failed to cast value {repr(value)} to type {hint}. Will "
                f"continue without casting, but this may cause errors in any "
                f"called code. Error was: {e}"
            )
            return value


T = TypeVar("T")


def mapping_to_named_tuple(mapping: Mapping, named_tuple: Type[T]) -> T:
    """Convert a dictionary-like object into the given named tuple

    This conversion is performed recursively. If the passed named tuple
    class contains child named tuples, then the the corresponding
    child keys of the dictionary will be mapped.

    This is used to take the supplied configuration and load it into the
    expected configuration structures.
    """
    import lightbus.config.structure

    hints = get_type_hints(named_tuple, None, lightbus.config.structure.__dict__)
    parameters = {}

    if mapping is None:
        return None

    for key, hint in hints.items():
        is_class = inspect.isclass(hint)
        value = mapping.get(key)

        if key not in mapping:
            continue

        # Is this an Optional[] hint (which looks like Union[Thing, None])
        subs_tree = hint._subs_tree() if hasattr(hint, "_subs_tree") else None
        if is_optional(hint) and value is not None:
            hint = subs_tree[1]

        if type_is_namedtuple(hint):
            parameters[key] = mapping_to_named_tuple(value, hint)
        elif is_class and issubclass(hint, Mapping) and subs_tree and len(subs_tree) == 3:
            parameters[key] = dict()
            for k, v in value.items():
                parameters[key][k] = mapping_to_named_tuple(v, hint._subs_tree()[2])
        else:
            parameters[key] = cast_to_hint(value, hint)

    return named_tuple(**parameters)
