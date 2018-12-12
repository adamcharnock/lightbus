from collections import OrderedDict
from datetime import datetime, timezone, date
from decimal import Decimal
from enum import Enum
from typing import NamedTuple
from uuid import UUID

import pytest
from dataclasses import dataclass

from lightbus.exceptions import DeformError
from lightbus.utilities.deforming import deform_to_bus
from lightbus.utilities.frozendict import frozendict

pytestmark = pytest.mark.unit


class SimpleNamedTuple(NamedTuple):
    a: str
    b: int


@dataclass
class SimpleDataclass(object):
    a: str
    b: int


class ComplexNamedTuple(NamedTuple):
    val: SimpleNamedTuple


@dataclass
class ComplexDataclass(object):
    val: SimpleDataclass


@dataclass
class DataclassWithMethod(object):
    a: str
    b: int

    def bar(self):
        pass


class CustomClass(object):
    pass


class CustomClassWithMagicMethod(object):
    def __to_bus__(self):
        return {"a": 1}


class CustomClassWithMagicMethodNeedsEncoding(object):
    def __to_bus__(self):
        return {"a": complex(1, 2)}


class ExampleEnum(Enum):
    foo: str = "a"
    bar: str = "b"


@pytest.mark.parametrize(
    "test_input,expected",
    [
        (1, 1),
        (1.23, 1.23),
        (True, True),
        (None, None),
        ("a", "a"),
        ([1, "a"], [1, "a"]),
        ({1}, [1]),
        (ExampleEnum.foo, "a"),
        (SimpleNamedTuple(a="x", b=1), {"a": "x", "b": 1}),
        (SimpleDataclass(a="x", b=1), {"a": "x", "b": 1}),
        (ComplexNamedTuple(val=SimpleNamedTuple(a="x", b=1)), {"val": {"a": "x", "b": 1}}),
        (ComplexDataclass(val=SimpleDataclass(a="x", b=1)), {"val": {"a": "x", "b": 1}}),
        (DataclassWithMethod(a="x", b=1), {"a": "x", "b": 1}),
        (
            datetime(2018, 6, 5, 10, 48, 12, 792_937, tzinfo=timezone.utc),
            "2018-06-05T10:48:12.792937+00:00",
        ),
        (date(2018, 6, 5), "2018-06-05"),
        (CustomClassWithMagicMethod(), {"a": 1}),
        (CustomClassWithMagicMethodNeedsEncoding(), {"a": "(1+2j)"}),
        (Decimal("1.23"), "1.23"),
        (complex(1, 2), "(1+2j)"),
        (frozendict(a=1), {"a": 1}),
        (UUID("abf4ddeb-fb9c-44c5-b865-012ba7787469"), "abf4ddeb-fb9c-44c5-b865-012ba7787469"),
        ({"a": 1}, {"a": 1}),
        ({"val": ExampleEnum.foo}, {"val": "a"}),
        (OrderedDict({"val": ExampleEnum.foo}), {"val": "a"}),
        ({ExampleEnum.foo}, ["a"]),
        ((ExampleEnum.foo,), ["a"]),
    ],
    ids=[
        "int",
        "float",
        "bool",
        "none",
        "str",
        "list",
        "set",
        "enum",
        "namedtuple_simple",
        "dataclass_simple",
        "namedtuple_complex",
        "dataclass_complex",
        "dataclass_with_method",
        "datetime",
        "date",
        "custom_class_magic_method",
        "custom_class_magic_method_needs_encoding",
        "decimal",
        "complex",
        "frozendict",
        "uuid",
        "dict",
        "dict_with_types",
        "orderd_dict_with_types",
        "set_enum",
        "tuple_enum",
    ],
)
def test_deform_to_bus(test_input, expected):
    actual = deform_to_bus(test_input)
    assert actual == expected
    assert type(actual) == type(expected)


def test_deform_to_bus_custom_object():
    obj = CustomClass()
    with pytest.raises(DeformError):
        assert deform_to_bus(obj) == obj
