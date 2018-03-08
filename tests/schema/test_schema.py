import asyncio
import json

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


@pytest.mark.run_loop
async def test_add_api(loop, redis_client, redis_pool):
    class TestApi(Api):
        my_event = Event(['field'])

        class Meta:
            name = 'my.test_api'

    schema = Schema(
        schema_transport=RedisSchemaTransport(redis_pool=redis_pool),
    )
    await schema.add_api(TestApi())
    assert await redis_client.exists('schemas')
    assert await redis_client.smembers('schemas') == [b'my.test_api']


@pytest.mark.run_loop
async def test_store(loop, redis_client, redis_pool):
    schema = Schema(
        schema_transport=RedisSchemaTransport(redis_pool=redis_pool),
    )
    schema.local_schemas['my.test_api'] = {'foo': 'bar'}
    await schema.store()
    assert await redis_client.exists('schemas')
    assert await redis_client.smembers('schemas') == [b'my.test_api']
    assert json.loads(await redis_client.get('schema:my.test_api')) == {'foo': 'bar'}


@pytest.mark.run_loop
async def test_monitor_store(loop, redis_client, redis_pool):
    """Check the monitor will persist local changes"""
    class TestApi(Api):
        my_event = Event(['field'])

        class Meta:
            name = 'my.test_api'

    schema = Schema(
        schema_transport=RedisSchemaTransport(redis_pool=redis_pool),
    )
    monitor_task = asyncio.ensure_future(schema.monitor(interval=0.1), loop=loop)

    assert await redis_client.smembers('schemas') == []
    await schema.add_api(TestApi())
    await asyncio.sleep(0.2)
    assert await redis_client.smembers('schemas') == [b'my.test_api']


@pytest.mark.run_loop
async def test_monitor_load(loop, redis_client, redis_pool):
    """Check the monitor will load new data from redis"""
    schema = Schema(
        schema_transport=RedisSchemaTransport(redis_pool=redis_pool),
    )
    monitor_task = asyncio.ensure_future(schema.monitor(interval=0.1), loop=loop)

    assert await redis_client.smembers('schemas') == []
    await redis_client.sadd('schemas', 'my.test_api')
    await redis_client.set('schemas:my.test_api', '{"foo": "bar"}')
    await asyncio.sleep(0.2)
    assert await redis_client.smembers('schemas') == [b'my.test_api']
    assert json.loads(await redis_client.get('schemas:my.test_api')) == {'foo': 'bar'}
