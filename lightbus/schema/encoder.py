from json import JSONEncoder

from datetime import datetime, date
from enum import Enum
from uuid import UUID

from lightbus.utilities.frozendict import frozendict


class LightbusEncoder(JSONEncoder):
    """Custom JSON encode to handle encoding some extra types

    See Also: json_safe_values()
    """

    def default(self, o):
        # If adding a type here, also add it to json_safe_values()
        if isinstance(o, frozendict):
            return o._dict
        elif isinstance(o, Enum):
            return o.value
        elif isinstance(o, (datetime, date)):
            return o.isoformat()
        elif isinstance(o, UUID):
            return str(o)
        else:
            super(LightbusEncoder, self).default(o)


def json_encode(obj, indent=2, sort_keys=True, **options):
    return LightbusEncoder(indent=indent, sort_keys=sort_keys, **options).encode(obj)


def json_safe_values(v):
    """Take a dictionary and do a best-effort at making the values json-safe

    This is useful when validating paramters and rpc return values against
    the json schema. Certainly on outgoing messages some values will
    be encodable by LightbusEncoder (above) but will not be recognised by the
    schema validator.

    We could do a full encode-decode cycle, but encoding data to a string
    only to decode it again seems wasteful. Let's just walk the dictionary instead
    """
    if isinstance(v, dict):
        for dict_key, dict_value in v.items():
            v[dict_key] = json_safe_values(dict_value)
        return v
    elif isinstance(v, frozendict):
        return v._dict
    elif isinstance(v, Enum):
        return v.value
    elif isinstance(v, (datetime, date)):
        return v.isoformat()
    elif isinstance(v, UUID):
        return str(v)
    elif type(v) == tuple:
        return list(v)
    else:
        return v
