from datetime import datetime, timezone

from enum import Enum

import pytest

from lightbus.schema.encoder import json_encode
from lightbus.utilities.frozendict import frozendict

pytestmark = pytest.mark.unit


def test_json_encode_datetime():
    dt = datetime(2000, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
    assert json_encode(dt, indent=None) == '"2000-01-01T00:00:00+00:00"'


def test_json_encode_frozendict():
    assert json_encode(frozendict(a=1), indent=None) == '{"a": 1}'


def test_json_encode_enum():
    class Color(Enum):
        red = 'red'
    assert json_encode(Color.red, indent=None) == '"red"'
