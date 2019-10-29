import logging
from datetime import datetime, timezone, date
from decimal import Decimal
from enum import Enum
from typing import NamedTuple, Optional, List, Any, SupportsRound, Mapping, Set, Tuple
from uuid import UUID

import pytest
from dataclasses import dataclass

from lightbus.transports.redis import redis_stream_id_subtract_one
from lightbus.utilities.casting import cast_to_signature, cast_to_hint
from lightbus.utilities.frozendict import frozendict

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


class SimpleNamedTuple(NamedTuple):
    a: str
    b: int


@dataclass
class SimpleDataclass(object):
    a: str
    b: int


class ExampleEnum(Enum):
    foo: str = "a"
    bar: str = "b"


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


class NamedTupleWithMapping(NamedTuple):
    a: dict
    b: Mapping
    c: Mapping[str, int]


@dataclass
class DataclassWithMapping(object):
    a: dict
    b: Mapping
    c: Mapping[str, int]


class NamedTupleWithMappingToNamedTuple(NamedTuple):
    a: Mapping[str, SimpleNamedTuple]


@dataclass
class DataclassWithMappingToDataclass(object):
    a: Mapping[str, SimpleDataclass]


@dataclass
class DataclassWithNoneDefault(object):
    a: SimpleDataclass = None


@dataclass
class DataclassWithChildNoneDefault(object):
    z: DataclassWithNoneDefault


class CustomClass(object):
    pass


class CustomClassWithMagicMethod(object):
    value: str = "123"

    @classmethod
    def __from_bus__(cls, data):
        o = cls()
        o.value = data["value"]
        return o


class NamedTupleWithMappingToCustomObject(NamedTuple):
    a: Mapping[str, CustomClassWithMagicMethod]


@dataclass
class DataclassWithMappingToCustomObject(object):
    a: Mapping[str, CustomClassWithMagicMethod]


# @dataclass
# class LedgerRecord(object):
#     uuid: UUID
#     message: str
#     timestamp: datetime
#     user: Optional[UUID]
#     actions: Dict[str, _url]
#     entities: List[Tuple[_verb, LedgerEntity]] = tuple()

_verb = str


@dataclass
class NamedTupleWithMappingToNestedTuple(object):
    a: List[Tuple[_verb, int]] = tuple()


@pytest.mark.parametrize(
    "test_input,hint,expected",
    [
        (1, int, 1),
        ("1", int, 1),
        (1.23, float, 1.23),
        ("1.23", float, 1.23),
        (True, bool, True),
        ("1", bool, True),
        ("", bool, False),
        (None, int, None),
        ("a", str, "a"),
        ("YWJjAA==", bytes, b"abc\0"),
        (1, str, "1"),
        ("1.23", Decimal, Decimal("1.23")),
        ("(1+2j)", complex, complex("(1+2j)")),
        ({"a": 1}, frozendict, frozendict(a=1)),
        (
            "abf4ddeb-fb9c-44c5-b865-012ba7787469",
            UUID,
            UUID("abf4ddeb-fb9c-44c5-b865-012ba7787469"),
        ),
        ({"a": 1, "b": "2"}, SimpleNamedTuple, SimpleNamedTuple(a="1", b=2)),
        ({"a": 1, "b": "2"}, SimpleDataclass, SimpleDataclass(a="1", b=2)),
        ({"a": 1, "b": "2"}, DataclassWithMethod, DataclassWithMethod(a="1", b=2)),
        (
            {"val": {"a": 1, "b": "2"}},
            ComplexNamedTuple,
            ComplexNamedTuple(val=SimpleNamedTuple(a="1", b=2)),
        ),
        (
            {"val": {"a": 1, "b": "2"}},
            ComplexDataclass,
            ComplexDataclass(val=SimpleDataclass(a="1", b=2)),
        ),
        (
            "2018-06-05T10:48:12.792937+00:00",
            datetime,
            datetime(2018, 6, 5, 10, 48, 12, 792_937, tzinfo=timezone.utc),
        ),
        ("2018-06-05", date, date(2018, 6, 5)),
        ("123", Any, "123"),
        ("123", Optional[int], 123),
        (None, Optional[int], None),
        (["1", 2], list, ["1", 2]),
        (["1", 2], List, ["1", 2]),
        (["1", 2], List[int], [1, 2]),
        (["1", 2], set, {"1", 2}),
        (["1", 2], Set, {"1", 2}),
        (["1", 2], Set[int], {1, 2}),
        (["1", 2], Tuple, ("1", 2)),
        (["1", "a", "2"], Tuple[int, str, int], (1, "a", 2)),
        (
            {"a": [["a", "1"], ["b", 2]]},
            NamedTupleWithMappingToNestedTuple,
            NamedTupleWithMappingToNestedTuple(a=[("a", 1), ("b", 2)]),
        ),
        ("a", ExampleEnum, ExampleEnum.foo),
        (
            {"a": {"z": 1}, "b": {"z": 1}, "c": {"z": 1}},
            NamedTupleWithMapping,
            NamedTupleWithMapping(a={"z": 1}, b={"z": 1}, c={"z": 1}),
        ),
        (
            {"a": {"z": 1}, "b": {"z": 1}, "c": {"z": 1}},
            DataclassWithMapping,
            DataclassWithMapping(a={"z": 1}, b={"z": 1}, c={"z": 1}),
        ),
        (
            {"a": {"foo": {"a": 1, "b": "2"}}},
            NamedTupleWithMappingToNamedTuple,
            NamedTupleWithMappingToNamedTuple(a={"foo": SimpleNamedTuple(a="1", b=2)}),
        ),
        (
            {"a": {"foo": {"a": 1, "b": "2"}}},
            DataclassWithMappingToDataclass,
            DataclassWithMappingToDataclass(a={"foo": SimpleDataclass(a="1", b=2)}),
        ),
        ({"a": None}, DataclassWithNoneDefault, DataclassWithNoneDefault(a=None)),
        (
            {"z": {"a": None}},
            DataclassWithChildNoneDefault,
            DataclassWithChildNoneDefault(DataclassWithNoneDefault(a=None)),
        ),
    ],
    ids=[
        "int_same",
        "int_cast",
        "float_same",
        "float_cast",
        "bool_same",
        "bool_cast_true",
        "bool_cast_false",
        "none",
        "str_same",
        "bytes",
        "str_cast",
        "decimal",
        "complex",
        "frozendict",
        "uuid",
        "nametuple",
        "dataclass",
        "dataclass_with_method",
        "complex_namedtuple",
        "complex_dataclass",
        "datetime",
        "date",
        "str_any",
        "optional_int_present",
        "optional_int_none",
        "list_builtin",
        "list_generic_untyped",
        "list_generic_typed",
        "set_builtin",
        "set_generic_untyped",
        "set_generic_typed",
        "tuple_generic_untyped",
        "tuple_generic_typed",
        "tuple_generic_nested_typed",
        "enum",
        "nametuple_with_mapping",
        "dataclass_with_mapping",
        "namedtuple_with_mapping_to_namedtuple",
        "dataclass_with_mapping_to_dataclass",
        "dataclass_with_none_default",
        "dataclass_with_child_none_default",
    ],
)
def test_cast_to_hit(test_input, hint, expected, caplog):
    with caplog.at_level(logging.WARNING, logger=""):
        casted = cast_to_hint(value=test_input, hint=hint)
    assert casted == expected

    # Check there were no warnings
    assert not caplog.records, "\n".join(r.message for r in caplog.records)


@pytest.mark.parametrize(
    "test_input,hint,expected",
    [
        ("a", int, "a"),
        ("a", float, "a"),
        (True, list, True),
        (True, tuple, True),
        (True, dict, True),
        (True, set, True),
        (True, datetime, True),
        # Custom classes not supported, so the hint is ignored
        ("a", CustomClass, "a"),
        (["1", 2], SupportsRound, ["1", 2]),
        ("x", ExampleEnum, "x"),
        (True, List[int], True),
        (True, Set[int], True),
        (True, Tuple[int, str, int], True),
    ],
    ids=[
        "str_int",
        "str_float",
        "bool_list",
        "bool_tuple",
        "bool_dict",
        "bool_set",
        "bool_datetime",
        "custom_class",
        "unsupported_generic",
        "enum_bad_value",
        "list_generic_typed_weird_input",
        "set_generic_typed_weird_input",
        "tuple_generic_nested_typed_weird_input",
    ],
)
def test_cast_to_annotation_with_warnings(test_input, hint, expected, caplog):
    with caplog.at_level(logging.WARNING, logger=""):
        casted = cast_to_hint(value=test_input, hint=hint)
        assert casted == expected

        # Check there were some warnings
        assert caplog.records


def test_cast_to_annotation_custom_class_with_magic_method():
    casted = cast_to_hint(value={"value": "abc"}, hint=CustomClassWithMagicMethod)
    assert isinstance(casted, CustomClassWithMagicMethod)
    assert casted.value == "abc"


def test_cast_to_annotation_custom_class_with_magic_method_on_mapping():
    casted = cast_to_hint({"a": {"b": {"value": "abc"}}}, hint=NamedTupleWithMappingToCustomObject)
    assert isinstance(casted, NamedTupleWithMappingToCustomObject)
    assert isinstance(casted.a, Mapping)
    assert isinstance(casted.a["b"], CustomClassWithMagicMethod)
    assert casted.a["b"].value == "abc"
