import asyncio
import json
from enum import Enum
from pathlib import Path
from typing import NamedTuple

import jsonschema
import os
import pytest

from lightbus import Event, Api, Parameter, Schema
from lightbus.exceptions import InvalidApiForSchemaCreation, SchemaNotFound, ValidationError
from lightbus.schema.schema import api_to_schema
from lightbus.transports.redis import RedisSchemaTransport

pytestmark = pytest.mark.unit


# api_to_schema()


def test_api_to_schema_event_long_form():

    class TestApi(Api):
        my_event = Event([Parameter("field", bool)])

        class Meta:
            name = "my.test_api"

    schema = api_to_schema(TestApi())
    assert schema["events"]["my_event"] == {
        "parameters": {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "type": "object",
            "additionalProperties": False,
            "properties": {"field": {"type": "boolean"}},
            "required": ["field"],
            "title": "Event my.test_api.my_event parameters",
        }
    }


def test_api_to_schema_event_short_form():

    class TestApi(Api):
        my_event = Event(["field"])

        class Meta:
            name = "my.test_api"

    schema = api_to_schema(TestApi())
    assert schema["events"]["my_event"] == {
        "parameters": {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "type": "object",
            "additionalProperties": False,
            "properties": {"field": {}},
            "required": ["field"],
            "title": "Event my.test_api.my_event parameters",
        }
    }


def test_api_to_schema_event_private():
    """Properties starting with an underscore should be ignored"""

    class TestApi(Api):
        _my_event = Event(["field"])

        class Meta:
            name = "my.test_api"

    schema = api_to_schema(TestApi())
    assert not schema["events"]


def test_api_to_schema_rpc():

    class TestApi(Api):

        def my_proc(self, field: bool = True) -> str:
            pass

        class Meta:
            name = "my.test_api"

    schema = api_to_schema(TestApi())
    assert schema["rpcs"]["my_proc"] == {
        "parameters": {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "type": "object",
            "additionalProperties": False,
            "properties": {"field": {"type": "boolean", "default": True}},
            "title": "RPC my.test_api.my_proc() parameters",
        },
        "response": {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "title": "RPC my.test_api.my_proc() response",
            "type": "string",
        },
    }


def test_api_to_schema_rpc_private():
    """Methods starting with an underscore should be ignored"""

    class TestApi(Api):

        def _my_proc(self, field: bool = True) -> str:
            pass

        class Meta:
            name = "my.test_api"

    schema = api_to_schema(TestApi())
    assert not schema["rpcs"]


def test_api_to_schema_class_not_instance():

    class TestApi(Api):

        class Meta:
            name = "my.test_api"

    with pytest.raises(InvalidApiForSchemaCreation):
        api_to_schema(TestApi)


# Schema class


@pytest.mark.asyncio
async def test_add_api(loop, schema, redis_client):

    class TestApi(Api):
        my_event = Event(["field"])

        class Meta:
            name = "my.test_api"

    await schema.add_api(TestApi())
    assert await redis_client.exists("schemas")
    assert await redis_client.smembers("schemas") == [b"my.test_api"]


@pytest.mark.asyncio
async def test_store(loop, schema, redis_client):
    schema.local_schemas["my.test_api"] = {"foo": "bar"}
    await schema.save_to_bus()
    assert await redis_client.exists("schemas")
    assert await redis_client.smembers("schemas") == [b"my.test_api"]
    assert json.loads(await redis_client.get("schema:my.test_api")) == {"foo": "bar"}


@pytest.mark.asyncio
async def test_monitor_store(loop, schema, redis_client):
    """Check the monitor will persist local changes"""

    class TestApi(Api):
        my_event = Event(["field"])

        class Meta:
            name = "my.test_api"

    monitor_task = asyncio.ensure_future(schema.monitor(interval=0.1))

    assert await redis_client.smembers("schemas") == []
    await schema.add_api(TestApi())
    await asyncio.sleep(0.2)
    assert await redis_client.smembers("schemas") == [b"my.test_api"]


@pytest.mark.asyncio
async def test_monitor_load(loop, schema, redis_client):
    """Check the monitor will load new data from redis"""
    monitor_task = asyncio.ensure_future(schema.monitor(interval=0.1))

    assert await redis_client.smembers("schemas") == []
    await redis_client.sadd("schemas", "my.test_api")
    await redis_client.set("schemas:my.test_api", '{"foo": "bar"}')
    await asyncio.sleep(0.2)
    assert await redis_client.smembers("schemas") == [b"my.test_api"]
    assert json.loads(await redis_client.get("schemas:my.test_api")) == {"foo": "bar"}


def test_save_local_file_empty(tmp_file, schema):
    schema.save_local(tmp_file.name)
    tmp_file.seek(0)
    assert tmp_file.read() == "{}"


@pytest.mark.asyncio
async def test_save_local_file(tmp_file, schema):

    class TestApi(Api):
        my_event = Event(["field"])

        class Meta:
            name = "my.test_api"

    await schema.add_api(TestApi())
    schema.save_local(tmp_file.name)
    tmp_file.seek(0)
    written_schema = tmp_file.read()
    assert len(written_schema) > 100
    assert "my.test_api" in json.loads(written_schema)


@pytest.mark.asyncio
async def test_save_local_file_remote_api(tmp_file, schema):
    # Ensure remote APIs are loaded and included in the save
    await schema.schema_transport.store("my.test_api", {"a": 1}, ttl_seconds=60)
    await schema.load_from_bus()
    schema.save_local(tmp_file.name)
    tmp_file.seek(0)
    written_schema = tmp_file.read()
    assert "my.test_api" in json.loads(written_schema)


def test_save_local_directory_empty(tmp_directory, schema):
    schema.save_local(tmp_directory)
    assert not os.listdir(tmp_directory)


@pytest.mark.asyncio
async def test_save_local_directory(tmp_directory, schema):

    class TestApi(Api):
        my_event = Event(["field"])

        class Meta:
            name = "my.test_api"

    await schema.add_api(TestApi())
    schema.save_local(tmp_directory)
    assert set(os.listdir(tmp_directory)) == {"my.test_api.json"}
    file_path = Path(tmp_directory) / "my.test_api.json"
    written_schema = file_path.read_text()
    assert len(written_schema) > 100
    assert "my.test_api" in json.loads(written_schema)


def test_load_local_file_path(tmp_file, schema):
    tmp_file.write('{"a": 1}')
    tmp_file.flush()
    schema.load_local(tmp_file.name)
    assert schema.local_schemas == {"a": 1}
    assert schema.remote_schemas == {}


def test_load_local_file_handle(tmp_file, schema):
    tmp_file.write('{"a": 1}')
    tmp_file.flush()
    tmp_file.seek(0)
    schema.load_local(tmp_file)
    assert schema.local_schemas == {"a": 1}
    assert schema.remote_schemas == {}


def test_load_local_directory(tmp_directory, schema):
    (tmp_directory / "test1.json").write_text('{"a": 1}')
    (tmp_directory / "test2.json").write_text('{"b": 2}')
    schema.load_local(tmp_directory)
    assert schema.local_schemas == {"a": 1, "b": 2}
    assert schema.remote_schemas == {}


@pytest.mark.asyncio
async def test_get_event_schema_found(schema, TestApi):
    await schema.add_api(TestApi())
    assert schema.get_event_schema("my.test_api", "my_event")


@pytest.mark.asyncio
async def test_get_event_schema_not_found(schema, TestApi):
    await schema.add_api(TestApi())
    with pytest.raises(SchemaNotFound):
        schema.get_event_schema("my.test_api", "foo")


@pytest.mark.asyncio
async def test_get_rpc_schema_found(schema, TestApi):
    await schema.add_api(TestApi())
    assert schema.get_rpc_schema("my.test_api", "my_proc")


@pytest.mark.asyncio
async def test_get_rpc_schema_not_found(schema, TestApi):
    await schema.add_api(TestApi())
    with pytest.raises(SchemaNotFound):
        schema.get_rpc_schema("my.test_api", "foo")


@pytest.mark.asyncio
async def test_get_event_or_rpc_schema_event_found(schema, TestApi):
    await schema.add_api(TestApi())
    assert schema.get_event_or_rpc_schema("my.test_api", "my_event")


@pytest.mark.asyncio
async def test_get_event_or_rpc_schema_event_not_found(schema, TestApi):
    await schema.add_api(TestApi())
    with pytest.raises(SchemaNotFound):
        schema.get_event_or_rpc_schema("my.test_api", "foo")


@pytest.mark.asyncio
async def test_get_event_or_rpc_schema_rpc_found(schema, TestApi):
    await schema.add_api(TestApi())
    assert schema.get_event_or_rpc_schema("my.test_api", "my_proc")


@pytest.mark.asyncio
async def test_get_event_or_rpc_schema_rpc_not_found(schema, TestApi):
    await schema.add_api(TestApi())
    with pytest.raises(SchemaNotFound):
        schema.get_event_or_rpc_schema("my.test_api", "foo")


@pytest.mark.asyncio
async def test_validate_parameters_rpc_valid(schema, TestApi):
    await schema.add_api(TestApi())
    schema.validate_parameters("my.test_api", "my_proc", {"field": True})


@pytest.mark.asyncio
async def test_validate_parameters_rpc_invalid(schema, TestApi):
    await schema.add_api(TestApi())
    with pytest.raises(ValidationError):
        schema.validate_parameters("my.test_api", "my_proc", {"field": 123})


@pytest.mark.asyncio
async def test_validate_parameters_event_valid(schema, TestApi):
    await schema.add_api(TestApi())
    schema.validate_parameters("my.test_api", "my_event", {"field": True})


@pytest.mark.asyncio
async def test_validate_parameters_event_invalid(schema, TestApi):
    await schema.add_api(TestApi())
    with pytest.raises(ValidationError):
        schema.validate_parameters("my.test_api", "my_event", {"field": 123})


@pytest.mark.asyncio
async def test_validate_response_valid(schema, TestApi):
    await schema.add_api(TestApi())
    schema.validate_response("my.test_api", "my_proc", "string")


@pytest.mark.asyncio
async def test_validate_response_invalid(schema, TestApi):
    await schema.add_api(TestApi())
    with pytest.raises(ValidationError):
        schema.validate_response("my.test_api", "my_proc", 123)


# Test validation error message types


@pytest.mark.asyncio
async def test_parameter_validation_error_parameter_mismatch(schema, TestApi):
    # Test omitted parameter / unexpected parameter message
    await schema.add_api(TestApi())
    with pytest.raises(ValidationError) as ex_info:
        schema.validate_parameters("my.test_api", "my_event", {"abc": "xyz"})

    # Error message gives some sensible hints
    assert "unwanted parameter" in str(ex_info.value)
    assert "omitted a required" in str(ex_info.value)


@pytest.mark.asyncio
async def test_parameter_validation_error_bad_value(schema, TestApi):
    # Test incorrect value message
    await schema.add_api(TestApi())
    # field should be a bool
    with pytest.raises(ValidationError) as ex_info:
        schema.validate_parameters("my.test_api", "my_event", {"field": "xyz"})

    # Error message gives some sensible hints
    assert "invalid value for the" in str(ex_info.value)


@pytest.mark.asyncio
async def test_parameter_validation_error_bad_structure(schema):
    # A complex structure with an internal validation error
    class Address(NamedTuple):
        line_1: str

    class User(NamedTuple):
        address: Address

    class UserApi(Api):
        my_event = Event([Parameter("user", User)])

        class Meta:
            name = "my.user_api"

    await schema.add_api(UserApi())
    # field should be a bool
    with pytest.raises(ValidationError) as ex_info:
        schema.validate_parameters(
            "my.user_api", "my_event", {"user": {"address": {"line_1": True}}}
        )

    # Error message gives some sensible hints
    assert "internal structure" in str(ex_info.value)


@pytest.mark.asyncio
async def test_parameter_validation_error_response_type(schema, TestApi):
    # Response should be string, but we use a bool to cause an error
    await schema.add_api(TestApi())
    with pytest.raises(ValidationError) as ex_info:
        schema.validate_response("my.test_api", "my_proc", True)

    # Error message gives some sensible hints
    assert "incorrect type" in str(ex_info.value)


@pytest.mark.asyncio
async def test_parameter_validation_error_structure(schema, TestApi):
    # A complex structure with an internal validation error
    class Address(NamedTuple):
        line_1: str

    class User(NamedTuple):
        address: Address

    class UserApi(Api):

        class Meta:
            name = "my.user_api"

        def my_proc(self) -> User:
            pass

    await schema.add_api(UserApi())
    with pytest.raises(ValidationError) as ex_info:
        schema.validate_response("my.user_api", "my_proc", {"address": {"line_1": True}})

    # Error message gives some sensible hints
    assert "internal structure" in str(ex_info.value)


@pytest.mark.asyncio
async def test_api_names(tmp_file, schema):

    class TestApi1(Api):

        class Meta:
            name = "my.test_api1"

    class TestApi2(Api):

        class Meta:
            name = "my.test_api2"

    await schema.add_api(TestApi1())
    await schema.add_api(TestApi2())
    assert set(schema.api_names) == {"my.test_api1", "my.test_api2"}


@pytest.mark.asyncio
async def test_events(tmp_file, schema):

    class TestApi1(Api):
        my_event_a = Event(["field"])
        my_event_b = Event(["field"])

        class Meta:
            name = "my.test_api1"

    class TestApi2(Api):
        my_event_a = Event(["field"])

        class Meta:
            name = "my.test_api2"

    await schema.add_api(TestApi1())
    await schema.add_api(TestApi2())
    assert set(schema.events) == {
        ("my.test_api1", "my_event_a"),
        ("my.test_api1", "my_event_b"),
        ("my.test_api2", "my_event_a"),
    }


@pytest.mark.asyncio
async def test_rpcs(tmp_file, schema):

    class TestApi1(Api):

        class Meta:
            name = "my.test_api1"

        def rpc_a(self):
            pass

        def rpc_b(self):
            pass

    class TestApi2(Api):

        class Meta:
            name = "my.test_api2"

        def rpc_a(self):
            pass

    await schema.add_api(TestApi1())
    await schema.add_api(TestApi2())
    assert set(schema.rpcs) == {
        ("my.test_api1", "rpc_a"),
        ("my.test_api1", "rpc_b"),
        ("my.test_api2", "rpc_a"),
    }
