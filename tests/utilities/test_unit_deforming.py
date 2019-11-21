from collections import OrderedDict
from copy import copy
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
class SimpleDataclass:
    a: str
    b: int


class ComplexNamedTuple(NamedTuple):
    val: SimpleNamedTuple


@dataclass
class ComplexDataclass:
    val: SimpleDataclass


@dataclass
class DataclassWithMethod:
    a: str
    b: int

    def bar(self):
        pass


class CustomClass:
    def __eq__(self, other):
        # Used below for checking the object hasn't been mutated
        return other.__dict__ == self.__dict__


class CustomClassWithMagicMethod:
    def __to_bus__(self):
        return {"a": 1}

    def __eq__(self, other):
        # Used below for checking the object hasn't been mutated
        return other.__dict__ == self.__dict__


class CustomClassWithMagicMethodNeedsEncoding:
    def __to_bus__(self):
        return {"a": complex(1, 2)}

    def __eq__(self, other):
        # Used below for checking the object hasn't been mutated
        return other.__dict__ == self.__dict__


class ExampleEnum(Enum):
    foo: str = "a"
    bar: str = "b"


DEFORMATION_TEST_PARAMETERS = {
    "int": (1, 1),
    "float": (1.23, 1.23),
    "bool": (True, True),
    "none": (None, None),
    "str": ("a", "a"),
    "bytes": (b"abc\0", "YWJjAA=="),
    "list": ([1, "a"], [1, "a"]),
    "set": ({1}, [1]),
    "enum": (ExampleEnum.foo, "a"),
    "namedtuple_simple": (SimpleNamedTuple(a="x", b=1), {"a": "x", "b": 1}),
    "dataclass_simple": (SimpleDataclass(a="x", b=1), {"a": "x", "b": 1}),
    "namedtuple_complex": (
        ComplexNamedTuple(val=SimpleNamedTuple(a="x", b=1)),
        {"val": {"a": "x", "b": 1}},
    ),
    "dataclass_complex": (
        ComplexDataclass(val=SimpleDataclass(a="x", b=1)),
        {"val": {"a": "x", "b": 1}},
    ),
    "dataclass_with_method": (DataclassWithMethod(a="x", b=1), {"a": "x", "b": 1}),
    "datetime": (
        datetime(2018, 6, 5, 10, 48, 12, 792_937, tzinfo=timezone.utc),
        "2018-06-05T10:48:12.792937+00:00",
    ),
    "date": (date(2018, 6, 5), "2018-06-05"),
    "custom_class_magic_method": (CustomClassWithMagicMethod(), {"a": 1}),
    "custom_class_magic_method_needs_encoding": (
        CustomClassWithMagicMethodNeedsEncoding(),
        {"a": "(1+2j)"},
    ),
    "decimal": (Decimal("1.23"), "1.23"),
    "complex": (complex(1, 2), "(1+2j)"),
    "frozendict": (frozendict(a=1), {"a": 1}),
    "uuid": (UUID("abf4ddeb-fb9c-44c5-b865-012ba7787469"), "abf4ddeb-fb9c-44c5-b865-012ba7787469"),
    "dict": ({"a": 1}, {"a": 1}),
    "dict_with_types": ({"val": ExampleEnum.foo}, {"val": "a"}),
    "orderd_dict_with_types": (OrderedDict({"val": ExampleEnum.foo}), {"val": "a"}),
    "set_enum": ({ExampleEnum.foo}, ["a"]),
    "tuple_enum": ((ExampleEnum.foo,), ["a"]),
}


@pytest.mark.parametrize(
    "test_input,expected",
    list(DEFORMATION_TEST_PARAMETERS.values()),
    ids=list(DEFORMATION_TEST_PARAMETERS.keys()),
)
def test_deform_to_bus(test_input, expected):
    value_before = copy(test_input)
    actual = deform_to_bus(test_input)
    assert actual == expected
    assert type(actual) == type(expected)

    # Test input value has not been mutated
    assert value_before == test_input


def test_deform_to_bus_custom_object():
    obj = CustomClass()
    with pytest.raises(DeformError):
        assert deform_to_bus(obj) == obj
