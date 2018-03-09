import asyncio
import json
from pathlib import Path

import os
import pytest

from lightbus import Event, Api, Parameter, Schema
from lightbus.exceptions import InvalidApiForSchemaCreation
from lightbus.schema.schema import api_to_schema
from lightbus.transports.redis import RedisSchemaTransport

pytestmark = pytest.mark.unit


# api_to_schema()


def test_api_to_schema_event_long_form():
    class TestApi(Api):
        my_event = Event([Parameter('field', bool)])

        class Meta:
            name = 'my.test_api'

    schema = api_to_schema(TestApi())
    assert schema['events']['my_event'] == {
        'parameters': {
            '$schema': 'http://json-schema.org/draft-04/schema#',
            'type': 'object',
            'additionalProperties': False,
            'properties': {'field': {'type': 'boolean'}},
            'required': ['field'],
            'title': 'Event my.test_api.my_event parameters',
        }
    }


def test_api_to_schema_event_short_form():
    class TestApi(Api):
        my_event = Event(['field'])

        class Meta:
            name = 'my.test_api'

    schema = api_to_schema(TestApi())
    assert schema['events']['my_event'] == {
        'parameters': {
            '$schema': 'http://json-schema.org/draft-04/schema#',
            'type': 'object',
            'additionalProperties': False,
            'properties': {'field': {}},
            'required': ['field'],
            'title': 'Event my.test_api.my_event parameters',
        }
    }


def test_api_to_schema_event_private():
    """Properties starting with an underscore should be ignored"""
    class TestApi(Api):
        _my_event = Event(['field'])

        class Meta:
            name = 'my.test_api'

    schema = api_to_schema(TestApi())
    assert not schema['events']


def test_api_to_schema_rpc():
    class TestApi(Api):

        def my_proc(self, field: bool=True) -> str:
            pass

        class Meta:
            name = 'my.test_api'

    schema = api_to_schema(TestApi())
    assert schema['rpcs']['my_proc'] == {
        'parameters': {
            '$schema': 'http://json-schema.org/draft-04/schema#',
            'type': 'object',
            'additionalProperties': False,
            'properties': {'field': {'type': 'boolean', 'default': True}},
            'required': [],
            'title': 'RPC my.test_api.my_proc() parameters',
        },
        'response': {
            '$schema': 'http://json-schema.org/draft-04/schema#',
            'title': 'RPC my.test_api.my_proc() response',
            'type': 'string'
        }
    }


def test_api_to_schema_rpc_private():
    """Methods starting with an underscore should be ignored"""
    class TestApi(Api):

        def _my_proc(self, field: bool=True) -> str:
            pass

        class Meta:
            name = 'my.test_api'

    schema = api_to_schema(TestApi())
    assert not schema['rpcs']


def test_api_to_schema_class_not_instance():
    class TestApi(Api):
        class Meta:
            name = 'my.test_api'

    with pytest.raises(InvalidApiForSchemaCreation):
        api_to_schema(TestApi)


# Schema class


@pytest.mark.run_loop
async def test_add_api(loop, schema, redis_client):
    class TestApi(Api):
        my_event = Event(['field'])

        class Meta:
            name = 'my.test_api'

    await schema.add_api(TestApi())
    assert await redis_client.exists('schemas')
    assert await redis_client.smembers('schemas') == [b'my.test_api']


@pytest.mark.run_loop
async def test_store(loop, schema, redis_client):
    schema.local_schemas['my.test_api'] = {'foo': 'bar'}
    await schema.save_to_bus()
    assert await redis_client.exists('schemas')
    assert await redis_client.smembers('schemas') == [b'my.test_api']
    assert json.loads(await redis_client.get('schema:my.test_api')) == {'foo': 'bar'}


@pytest.mark.run_loop
async def test_monitor_store(loop, schema, redis_client):
    """Check the monitor will persist local changes"""
    class TestApi(Api):
        my_event = Event(['field'])

        class Meta:
            name = 'my.test_api'

    monitor_task = asyncio.ensure_future(schema.monitor(interval=0.1), loop=loop)

    assert await redis_client.smembers('schemas') == []
    await schema.add_api(TestApi())
    await asyncio.sleep(0.2)
    assert await redis_client.smembers('schemas') == [b'my.test_api']


@pytest.mark.run_loop
async def test_monitor_load(loop, schema, redis_client):
    """Check the monitor will load new data from redis"""
    monitor_task = asyncio.ensure_future(schema.monitor(interval=0.1), loop=loop)

    assert await redis_client.smembers('schemas') == []
    await redis_client.sadd('schemas', 'my.test_api')
    await redis_client.set('schemas:my.test_api', '{"foo": "bar"}')
    await asyncio.sleep(0.2)
    assert await redis_client.smembers('schemas') == [b'my.test_api']
    assert json.loads(await redis_client.get('schemas:my.test_api')) == {'foo': 'bar'}


def test_save_local_file_empty(tmp_file, schema):
    schema.save_local(tmp_file.name)
    tmp_file.seek(0)
    assert tmp_file.read() == '{}'


@pytest.mark.run_loop
async def test_save_local_file(tmp_file, schema):
    class TestApi(Api):
        my_event = Event(['field'])

        class Meta:
            name = 'my.test_api'

    await schema.add_api(TestApi())
    schema.save_local(tmp_file.name)
    tmp_file.seek(0)
    written_schema = tmp_file.read()
    assert len(written_schema) > 100
    assert 'my.test_api' in json.loads(written_schema)


@pytest.mark.run_loop
async def test_save_local_file_remote_api(tmp_file, schema):
    # Ensure remote APIs are loaded and included in the save
    await schema.schema_transport.store('my.test_api', {'a': 1}, ttl_seconds=60)
    await schema.load_from_bus()
    schema.save_local(tmp_file.name)
    tmp_file.seek(0)
    written_schema = tmp_file.read()
    assert 'my.test_api' in json.loads(written_schema)


def test_save_local_directory_empty(tmp_directory, schema):
    schema.save_local(tmp_directory)
    assert not os.listdir(tmp_directory)


@pytest.mark.run_loop
async def test_save_local_directory(tmp_directory, schema):
    class TestApi(Api):
        my_event = Event(['field'])

        class Meta:
            name = 'my.test_api'

    await schema.add_api(TestApi())
    schema.save_local(tmp_directory)
    assert set(os.listdir(tmp_directory)) == {'my.test_api.json'}
    file_path = Path(tmp_directory) / 'my.test_api.json'
    written_schema = file_path.read_text()
    assert len(written_schema) > 100
    assert 'my.test_api' in json.loads(written_schema)


def test_load_local_file_path(tmp_file, schema):
    tmp_file.write('{"a": 1}')
    tmp_file.flush()
    schema.load_local(tmp_file.name)
    assert schema.local_schemas == {'a': 1}
    assert schema.remote_schemas == {}


def test_load_local_file_handle(tmp_file, schema):
    tmp_file.write('{"a": 1}')
    tmp_file.flush()
    tmp_file.seek(0)
    schema.load_local(tmp_file)
    assert schema.local_schemas == {'a': 1}
    assert schema.remote_schemas == {}


def test_load_local_directory(tmp_directory, schema):
    (tmp_directory / 'test1.json').write_text('{"a": 1}')
    (tmp_directory / 'test2.json').write_text('{"b": 2}')
    schema.load_local(tmp_directory)
    assert schema.local_schemas == {'a': 1, 'b': 2}
    assert schema.remote_schemas == {}
