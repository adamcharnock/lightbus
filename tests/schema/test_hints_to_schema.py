import functools
import inspect
import logging
from decimal import Decimal
from typing import Union, Optional, NamedTuple, Tuple, Any, Mapping, Set, Dict
from collections import namedtuple
from uuid import UUID

import jsonschema
import pytest
from datetime import datetime, date, time
from enum import Enum

from lightbus.schema.hints_to_schema import (
    make_parameter_schema,
    python_type_to_json_schemas,
    make_response_schema,
    make_rpc_parameter_schema,
)

pytestmark = pytest.mark.unit


def test_no_types():
    def func(username):
        pass

    schema = make_rpc_parameter_schema("api_name", "rpc_name", func)
    assert schema["properties"]["username"] == {}
    assert schema["required"] == ["username"]
    assert schema["additionalProperties"] is False


def test_default():
    def func(field=123):
        pass

    schema = make_rpc_parameter_schema("api_name", "rpc_name", func)
    assert schema["properties"]["field"] == {"type": "integer", "default": 123}
    # Has a default value, so not required
    assert "required" not in schema


def test_type():
    def func(field: dict):
        pass

    schema = make_rpc_parameter_schema("api_name", "rpc_name", func)
    assert schema["properties"]["field"] == {"type": "object"}
    assert schema["required"] == ["field"]


def test_type_with_default():
    def func(field: float = 3.142):
        pass

    schema = make_rpc_parameter_schema("api_name", "rpc_name", func)
    assert schema["properties"]["field"] == {"type": "number", "default": 3.142}
    assert "required" not in schema


def test_kwargs():
    def func(field: dict, **kwargs):
        pass

    schema = make_rpc_parameter_schema("api_name", "rpc_name", func)
    # **kwargs isn't a property, but additionalProperties is now set to true
    assert list(schema["properties"].keys()) == ["field"]
    assert schema["required"] == ["field"]
    assert schema["additionalProperties"] is True


def test_positional_args():
    def func(field: dict, *args):
        pass

    schema = make_rpc_parameter_schema("api_name", "rpc_name", func)
    assert list(schema["properties"].keys()) == ["field"]  # *args is ignored
    assert schema["required"] == ["field"]
    assert schema["additionalProperties"] is False


def test_python_type_to_json_types_union():
    json_types = python_type_to_json_schemas(Union[str, int])
    assert json_types == [{"type": "string"}, {"type": "integer"}]


def test_python_type_to_json_types_empty(caplog):
    with caplog.at_level(logging.CRITICAL, logger=""):
        json_types = python_type_to_json_schemas(inspect.Parameter.empty)
    assert json_types == [{}]
    # Check there are no warnings
    assert not caplog.records


def test_union():
    def func(field: Union[str, int]):
        pass

    schema = make_rpc_parameter_schema("api_name", "rpc_name", func)
    assert schema["properties"]["field"] == {"anyOf": [{"type": "string"}, {"type": "integer"}]}
    assert schema["required"] == ["field"]


def test_union_default():
    def func(field: Union[str, int] = 123):
        pass

    schema = make_rpc_parameter_schema("api_name", "rpc_name", func)
    assert schema["properties"]["field"] == {
        "anyOf": [
            {"type": "string", "default": 123},  # Technically an invalid default value
            {"type": "integer", "default": 123},
        ]
    }
    assert "required" not in schema


def test_optional():
    def func(username: Optional[str]):
        pass

    schema = make_rpc_parameter_schema("api_name", "rpc_name", func)
    assert schema["properties"]["username"] == {"anyOf": [{"type": "string"}, {"type": "null"}]}
    assert schema["required"] == ["username"]


def test_named_tuple():
    class User(NamedTuple):
        username: str
        password: str
        is_admin: bool = False

    def func(user: User):
        pass

    schema = make_rpc_parameter_schema("api_name", "rpc_name", func)

    assert schema["properties"]["user"]["type"] == "object"
    assert schema["properties"]["user"]["properties"] == {
        "username": {"type": "string"},
        "password": {"type": "string"},
        "is_admin": {"type": "boolean", "default": False},
    }
    assert set(schema["properties"]["user"]["required"]) == {"password", "username"}


def test_named_tuple_enum_with_default():
    class MyEnum(Enum):
        foo: int = 1
        bar: int = 2

    class User(NamedTuple):
        field: MyEnum = MyEnum.bar

    def func(user: User):
        pass

    schema = make_rpc_parameter_schema("api_name", "rpc_name", func)

    assert schema["properties"]["user"]["type"] == "object"
    assert schema["properties"]["user"]["properties"] == {
        "field": {"type": "integer", "enum": [1, 2], "default": 2}
    }
    assert "required" not in schema["properties"]["user"]


def test_named_tuple_using_function():

    User = namedtuple("User", ("username", "password"))

    def func(user: User):
        pass

    schema = make_rpc_parameter_schema("api_name", "rpc_name", func)

    assert schema["properties"]["user"]["type"] == "object"
    assert schema["properties"]["user"]["properties"] == {"username": {}, "password": {}}
    assert set(schema["properties"]["user"]["required"]) == {"password", "username"}


def test_response_no_types():
    def func(username):
        pass

    schema = make_response_schema("api_name", "rpc_name", func)
    assert "type" not in schema


def test_response_wrapped():
    def func() -> int:
        pass

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        pass

    schema = make_response_schema("api_name", "rpc_name", wrapper)
    assert schema["type"] == "integer"


def test_response_bool():
    def func(username) -> bool:
        pass

    schema = make_response_schema("api_name", "rpc_name", func)
    assert schema["type"] == "boolean"


def test_response_null():
    def func(username) -> None:
        pass

    schema = make_response_schema("api_name", "rpc_name", func)
    assert schema["type"] == "null"


def test_response_typed_tuple():
    def func(username) -> Tuple[str, int, bool]:
        pass

    schema = make_response_schema("api_name", "rpc_name", func)
    assert schema["type"] == "array"
    assert schema["items"] == [{"type": "string"}, {"type": "integer"}, {"type": "boolean"}]


def test_response_named_tuple():
    class User(NamedTuple):
        username: str
        password: str
        is_admin: bool = False

    def func(username) -> User:
        pass

    schema = make_response_schema("api_name", "rpc_name", func)
    assert schema["type"] == "object"
    assert schema["properties"] == {
        "username": {"type": "string"},
        "password": {"type": "string"},
        "is_admin": {"type": "boolean", "default": False},
    }
    assert set(schema["required"]) == {"username", "password"}
    assert schema["additionalProperties"] == False


def test_unknown_type():
    class UnknownThing:
        pass

    def func(username) -> UnknownThing:
        pass

    schema = make_response_schema("api_name", "rpc_name", func)
    assert "type" not in schema


def test_any():
    def func(username) -> Any:
        pass

    schema = make_response_schema("api_name", "rpc_name", func)
    assert "type" not in schema


def test_ellipsis():
    def func(username) -> ...:
        pass

    schema = make_response_schema("api_name", "rpc_name", func)
    assert "type" not in schema


def test_mapping_with_types():
    def func(username) -> Mapping[str, int]:
        pass

    schema = make_response_schema("api_name", "rpc_name", func)
    assert schema["type"] == "object"
    assert schema["additionalProperties"] == {"type": "integer"}


def test_mapping_without_types():
    def func(username) -> Mapping:
        pass

    schema = make_response_schema("api_name", "rpc_name", func)
    assert schema["type"] == "object"
    assert "patternProperties" not in schema
    assert "required" not in schema


def test_enum_number():
    TestEnum = Enum("ExampleEnum", {"Foo": 1, "Bar": 2})

    def func(username) -> TestEnum:
        pass

    schema = make_response_schema("api_name", "rpc_name", func)
    assert schema["type"] == "integer"
    assert set(schema["enum"]) == {1, 2}


def test_enum_string():
    TestEnum = Enum("ExampleEnum", {"Foo": "foo", "Bar": "bar"})

    def func(username) -> TestEnum:
        pass

    schema = make_response_schema("api_name", "rpc_name", func)
    assert schema["type"] == "string"
    assert set(schema["enum"]) == {"foo", "bar"}


def test_enum_empty():
    TestEnum = Enum("ExampleEnum", {})

    def func(username) -> TestEnum:
        pass

    schema = make_response_schema("api_name", "rpc_name", func)
    assert "type" not in schema
    assert "enum" not in schema


def test_enum_unknown_value_types():
    class UnknownThing:
        pass

    TestEnum = Enum("ExampleEnum", {"Foo": UnknownThing, "Bar": UnknownThing})

    def func(username) -> TestEnum:
        pass

    schema = make_response_schema("api_name", "rpc_name", func)
    assert "type" not in schema
    assert "enum" not in schema


def test_set_type():
    def func(username) -> Set:
        pass

    schema = make_response_schema("api_name", "rpc_name", func)
    assert schema["type"] == "array"
    assert "items" not in schema


def test_set_type_with_hints():
    def func(username) -> Set[int]:
        pass

    schema = make_response_schema("api_name", "rpc_name", func)
    assert schema["type"] == "array"
    assert schema["items"] == {"type": "integer"}


def test_set_builtin():
    def func(username) -> Set:
        pass

    schema = make_response_schema("api_name", "rpc_name", func)
    assert schema["type"] == "array"
    assert "items" not in schema


def test_datetime():
    def func(username) -> datetime:
        pass

    schema = make_response_schema("api_name", "rpc_name", func)
    assert schema["type"] == "string"
    assert schema["format"] == "date-time"


def test_date():
    def func(username) -> date:
        pass

    schema = make_response_schema("api_name", "rpc_name", func)
    assert schema["type"] == "string"
    assert schema["format"] == "date"


def test_time():
    def func(username) -> time:
        pass

    schema = make_response_schema("api_name", "rpc_name", func)
    assert schema["type"] == "string"
    assert schema["format"] == "time"


def test_decimal():
    def func(username) -> Decimal:
        pass

    schema = make_response_schema("api_name", "rpc_name", func)
    assert schema["type"] == "string"
    assert schema["pattern"]


def test_uuid():
    def func(username) -> UUID:
        pass

    schema = make_response_schema("api_name", "rpc_name", func)
    assert schema["type"] == "string"
    assert "pattern" not in schema


def test_object_with_method():
    class User:
        username: str

        def my_func(self):
            pass

    def func(username) -> User:
        pass

    schema = make_response_schema("api_name", "rpc_name", func)
    assert schema["type"] == "object"
    assert schema["properties"] == {"username": {"type": "string"}}
    assert set(schema["required"]) == {"username"}
    assert schema["additionalProperties"] == False


def test_object_with_property():
    class User:
        username: str

        @property
        def my_prop(self):
            pass

    def func(username) -> User:
        pass

    schema = make_response_schema("api_name", "rpc_name", func)
    assert schema["type"] == "object"
    assert schema["properties"]["my_prop"] == {}


def test_object_with_to_bus():
    class CustomClassToBus:
        def __to_bus__(self) -> float:
            pass

    def func(username) -> CustomClassToBus:
        pass

    schema = make_response_schema("api_name", "rpc_name", func)
    assert schema["type"] == "number"


def test_make_rpc_parameter_schema_null():
    def func(username=None):
        pass

    schema = make_rpc_parameter_schema("api_name", "rpc_name", func)
    # Note that type is not set to null
    assert schema["properties"]["username"] == {"default": None}


def test_make_rpc_parameter_schema_wrapped():
    """Ensure wrapped functions are unwrapped"""

    def func(field: int):
        pass

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        pass

    schema = make_rpc_parameter_schema("api_name", "rpc_name", wrapper)
    assert schema["properties"]["field"] == {"type": "integer"}


def test_named_tuple_with_none_default():
    class User(NamedTuple):
        pass

    def func(user: User = None):
        pass

    schema = make_rpc_parameter_schema("api_name", "rpc_name", func)
    assert len(schema["properties"]["user"]["anyOf"]) == 2


def test_named_tuple_optional_with_none_default():
    # There is a risk of {'type': 'null'} being present twice here,
    # resulting in three values in anyOf. Check this doesn't happen

    class User(NamedTuple):
        pass

    def func(user: Optional[User] = None):
        pass

    schema = make_rpc_parameter_schema("api_name", "rpc_name", func)
    assert len(schema["properties"]["user"]["anyOf"]) == 2


def test_named_tuple_field_with_none_default():
    class Child(NamedTuple):
        pass

    class User(NamedTuple):
        foo: Child = None

    def func(user: User):
        pass

    schema = make_rpc_parameter_schema("api_name", "rpc_name", func)
    assert len(schema["properties"]["user"]["properties"]["foo"]["anyOf"]) == 2


class _TestEnum(Enum):
    a = "xxx"
    b = "yyy"


class CustomClass:
    a: int
    b: str


@pytest.mark.parametrize(
    "test_input,type_",
    [
        # Simple types first
        ("foo", str),
        ("foo", bytes),
        ("(1+2j)", complex),
        ("76f37aaf-b399-4269-8187-5831edd76bef", UUID),
        ("1.23", Decimal),
        (True, bool),
        (False, bool),
        (123, int),
        (123.45, float),
        ({"a": 1}, dict),
        ({"a": 1}, Dict),
        ({"a": 1}, Dict[str, int]),
        ([1, 2], tuple),
        ([1, 2], Tuple),
        ([1, 2], Tuple[int, int]),
        ("xxx", _TestEnum),
        ([1, 2], set),
        ([1, 2], Set),
        ([1, 2], Set[int]),
        (None, None),
        ("2019-11-23T17:26:16+00:00", datetime),
        ("2019-11-23", date),
        ("10:23", time),
        ({"a": 1, "b": "a"}, CustomClass),
        # Unions
        ("foo", Union[str, int]),
        (1, Union[str, int]),
    ],
    ids=[
        "str",
        "bytes",
        "complex",
        "uuid",
        "decimal",
        "bool_true",
        "bool_false",
        "int",
        "float",
        "dict",
        "dict_typing",
        "dict_typed",
        "tuple",
        "tuple_typing",
        "tuple_typed",
        "enum",
        "set",
        "set_typing",
        "set_typed",
        "none",
        "datetime",
        "date",
        "time",
        "custom_class",
        "union_str",
        "union_int",
    ],
)
def test_validation_cycle(test_input, type_):
    schemas = python_type_to_json_schemas(type_)

    for schema in schemas:
        try:
            jsonschema.validate(test_input, schema)
        except jsonschema.ValidationError:
            # Try the next schema
            pass
        else:
            # Successfully validated against a schema, so stop
            return

    raise AssertionError(f"No schemas validated value {repr(test_input)}. Schemas: {schemas}")
