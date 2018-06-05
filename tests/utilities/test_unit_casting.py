from datetime import datetime, timezone, date
from typing import NamedTuple, Optional

import pytest
from dataclasses import dataclass

from lightbus.transports.redis import redis_stream_id_subtract_one
from lightbus.utilities.casting import cast_to_signature, cast_to_hint

pytestmark = pytest.mark.unit


def test_cast_to_signature_simple():

    def fn(a: int, b: str, c):
        pass

    obj = object()
    casted = cast_to_signature(callable=fn, parameters={"a": "1", "b": 2, "c": obj})
    assert casted == {"a": 1, "b": "2", "c": obj}


def test_cast_to_annotation_named_tuple():

    class Foo(NamedTuple):
        a: str
        b: int

    casted = cast_to_hint(value={"a": 1, "b": "2"}, hint=Foo)
    assert casted == Foo(a="1", b=2)


def test_cast_to_annotation_dataclass():

    @dataclass
    class Foo(object):
        a: str
        b: int

    casted = cast_to_hint(value={"a": 1, "b": "2"}, hint=Foo)
    assert casted == Foo(a="1", b=2)


def test_cast_to_annotation_dataclass_with_method():

    @dataclass
    class Foo(object):
        a: str
        b: int

        def bar(self):
            pass

    casted = cast_to_hint(value={"a": 1, "b": "2"}, hint=Foo)
    assert casted == Foo(a="1", b=2)


def test_cast_to_annotation_str_to_datetime():
    casted = cast_to_hint(value="2018-06-05T10:48:12.792937+00:00", hint=datetime)
    assert casted == datetime(2018, 6, 5, 10, 48, 12, 792937, tzinfo=timezone.utc)


def test_cast_to_annotation_str_to_date():
    casted = cast_to_hint(value="2018-06-05", hint=date)
    assert casted == date(2018, 6, 5)


def test_cast_to_annotation_none():
    casted = cast_to_hint(value=None, hint=int)
    assert casted is None


def test_cast_to_annotation_uncastable():
    casted = cast_to_hint(value="a", hint=int)
    assert casted == "a"


def test_cast_to_annotation_optional_present():
    casted = cast_to_hint(value="123", hint=Optional[int])
    assert casted == 123


def test_cast_to_annotation_optional_none():
    casted = cast_to_hint(value=None, hint=Optional[int])
    assert casted is None


def test_cast_to_annotation_custom_class():

    class CustomClass(object):
        pass

    casted = cast_to_hint(value="a", hint=CustomClass)
    # Custom classes not supported
    assert casted == "a"
