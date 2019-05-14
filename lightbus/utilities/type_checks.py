import inspect
import sys
from typing import Optional, Type, Union, Tuple, List, Sequence, TypeVar


def type_is_namedtuple(t) -> bool:
    """Figuring out if a type is a named tuple is not as trivial as one may expect"""
    try:
        return issubclass(t, tuple) and hasattr(t, "_fields")
    except TypeError:
        return False


def is_namedtuple(v) -> bool:
    """Figuring out if an object is a named tuple is not as trivial as one may expect"""
    try:
        return isinstance(v, tuple) and hasattr(v, "_fields")
    except TypeError:
        return False


def type_is_dataclass(t) -> bool:
    return hasattr(t, "__dataclass_fields__")


def is_dataclass(v) -> bool:
    return hasattr(v, "__dataclass_fields__")


def is_optional(hint) -> Optional[Type]:
    hint_type, hint_args = parse_hint(hint)
    if hint_type == Union and len(hint_args) == 2 and hint_args[1] == type(None):
        return hint_args[0]
    else:
        return None


def isinstance_safe(value, type_):
    """Determine if value is a subclass of type_

    Will work even if it is not a valid question to ask of
    the given value.
    """
    try:
        return isinstance(value, type_)
    except TypeError:
        # Cannot perform isinstance on some types
        return False


def issubclass_safe(value, type_):
    """Determine if value is a subclass of type_

    Will work even value is not a class
    """
    try:
        return issubclass(value, type_)
    except (TypeError, AttributeError):
        # Cannot perform issubclass on some types
        return False


def parse_hint(hint: Type) -> Tuple[Type, Optional[List]]:
    # TODO: Cleanup this logic (especially if/when python 3.6 support is dropped)
    if sys.version_info >= (3, 7):
        if hasattr(hint, "__origin__"):
            # Python 3.7, and this is a type hint (eg typing.Union)
            # Filter out TypeVars such as KT & VT_co (they generally
            # indicate that no explicit hint was given)
            hint_args = [a for a in hint.__args__ if not isinstance(a, TypeVar)]
            return hint.__origin__, hint_args or None
        else:
            # Python 3.7, but this is something other than a type hint
            # (e.g. an int or datetime)
            return hint, None
    else:
        if hasattr(hint, "_subs_tree"):
            # Python 3.6, and this is a type hint (eg typing.Union)
            subs_tree = hint._subs_tree()
            if isinstance(subs_tree, Sequence):
                # Type hint has sub types (e.g. Sequence[str])
                return subs_tree[0], hint.__args__
            else:
                # Type hint has no sub types (e.g. Sequence)
                return hint, None
        else:
            # Python 3.6, but this is something other than a type hint
            # (e.g. an int or datetime)
            return hint, None


def get_property_default(type: Type, property_name: str) -> ...:
    if issubclass_safe(type, tuple):
        # namedtuple
        if hasattr(type, "_field_defaults"):
            default = type._field_defaults.get(property_name, inspect.Parameter.empty)
        else:
            default = inspect.Parameter.empty
    else:
        # everything else
        default = getattr(type, property_name, inspect.Parameter.empty)

    if callable(default):
        default = inspect.Parameter.empty

    return default
