import inspect
from typing import Optional, Type, Union, Tuple, List, TypeVar


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
    """ Parse a typing hint into its type and and arguments.

    For example:

        >>> parse_hint(Union[dict, list])
        (typing.Union, [<class 'dict'>, <class 'list'>])

        >>> parse_hint(int)
        (<class 'int'>, None)

    """
    if hasattr(hint, "__origin__"):
        # This is a type hint (eg typing.Union)
        # Filter out TypeVars such as KT & VT_co (they generally
        # indicate that no explicit hint was given)
        hint_args = [a for a in hint.__args__ if not isinstance(a, TypeVar)]
        return hint.__origin__, hint_args or None
    else:
        # This is something other than a type hint
        # (e.g. an int or datetime)
        return hint, None


def get_property_default(type_: Type, property_name: str) -> ...:
    """ Get the default value for a property with name `property_name` on class `type_`"""
    if issubclass_safe(type_, tuple):
        # namedtuple
        if hasattr(type_, "_field_defaults"):
            default = type_._field_defaults.get(property_name, inspect.Parameter.empty)
        else:
            default = inspect.Parameter.empty
    else:
        # everything else
        default = getattr(type_, property_name, inspect.Parameter.empty)

    if callable(default):
        default = inspect.Parameter.empty

    return default
