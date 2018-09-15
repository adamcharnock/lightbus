from typing import Optional, Type, Union


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
    subs_tree = hint._subs_tree() if hasattr(hint, "_subs_tree") else []
    if type(hint) == type(Union) and len(subs_tree) == 3 and subs_tree[2] == type(None):
        return subs_tree[1]
    else:
        return None


def isinstance_safe(value, type_):
    try:
        return isinstance(value, type_)
    except TypeError:
        # Cannot perform isinstance on some types
        return False
