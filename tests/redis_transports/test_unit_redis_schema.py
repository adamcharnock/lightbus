import json

import pytest

from lightbus.transports.redis import RedisSchemaTransport

pytestmark = pytest.mark.unit


@pytest.mark.run_loop
async def test_store(redis_schema_transport: RedisSchemaTransport, redis_client):
    await redis_schema_transport.store('my.api', {'key': 'value'}, ttl_seconds=60)
    assert set(await redis_client.keys('*')) == {
        b'schemas',
        b'schema:my.api',
    }

    schemas = await redis_client.smembers('schemas')
    assert schemas == [b'my.api']

    my_api_schema = await redis_client.get('schema:my.api')
    assert json.loads(my_api_schema) == {'key': 'value'}

    ttl = await redis_client.ttl('schema:my.api')
    assert 59 <= ttl <= 60


@pytest.mark.run_loop
async def test_load(redis_schema_transport: RedisSchemaTransport, redis_client):
    await redis_client.sadd('schemas', 'my.api', 'old.api')
    await redis_client.set('schema:my.api', json.dumps({'key': 'value'}))


    schemas = await redis_schema_transport.load()

    assert schemas == {
        'my.api': {'key': 'value'}
    }
