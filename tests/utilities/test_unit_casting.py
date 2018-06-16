from datetime import datetime, timezone, date
from enum import Enum
from typing import NamedTuple, Optional, List, Any, SupportsRound

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


def test_cast_to_signature_no_annotations():

    def fn(a, b, c):
        pass

    obj = object()
    casted = cast_to_signature(callable=fn, parameters={"a": "1", "b": 2, "c": obj})
    # Values untouched
    assert casted == {"a": "1", "b": 2, "c": obj}


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


def test_cast_to_annotation_any():
    casted = cast_to_hint(value="123", hint=Any)
    assert casted == "123"


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


def test_cast_to_annotation_list_builtin():
    casted = cast_to_hint(value=["1", 2], hint=list)
    assert casted == ["1", 2]


def test_cast_to_annotation_list_generic_untyped():
    casted = cast_to_hint(value=["1", 2], hint=List)
    assert casted == ["1", 2]


def test_cast_to_annotation_list_generic_typed():
    casted = cast_to_hint(value=["1", 2], hint=List[int])
    assert casted == [1, 2]


def test_cast_to_annotation_unsupported_generic():
    casted = cast_to_hint(value=["1", 2], hint=SupportsRound)
    assert casted == ["1", 2]


def test_cast_to_annotation_enum():

    class MyEnum(Enum):
        foo: str = "a"
        bar: str = "b"

    casted = cast_to_hint(value="a", hint=MyEnum)
    assert casted is MyEnum.foo


def test_cast_to_annotation_enum_bad_value():

    class MyEnum(Enum):
        foo: str = "a"
        bar: str = "b"

    casted = cast_to_hint(value="x", hint=MyEnum)
    assert casted is "x"


def test_cast_to_annotation_magic_method():

    class MyClass(object):
        value: str = "123"

        @classmethod
        def __from_bus__(cls, data):
            o = cls()
            o.value = data["value"]
            return o

    casted = cast_to_hint(value={"value": "abc"}, hint=MyClass)
    assert casted.value == "abc"
