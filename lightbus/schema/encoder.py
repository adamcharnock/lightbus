from json import JSONEncoder

from datetime import datetime, date
from enum import Enum
from uuid import UUID

from lightbus.utilities.frozendict import frozendict


class LightbusEncoder(JSONEncoder):
    """Custom JSON encode to handle encoding some extra types"""

    def default(self, o):
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


def json_safe_values(d: dict) -> dict:
    for k, v in d.items():
        if isinstance(v, dict):
            d[k] = json_safe_values(v)
        elif isinstance(v, frozendict):
            d[k] = v._dict
        elif isinstance(v, Enum):
            d[k] = v.value
        elif isinstance(v, (datetime, date)):
            d[k] = v.isoformat()
        elif isinstance(v, UUID):
            d[k] = str(v)
    return d
