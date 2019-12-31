import json

import pytest

from lightbus import RedisSchemaTransport

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_store(redis_schema_transport: RedisSchemaTransport, redis_client):
    await redis_schema_transport.store("my.api", {"key": "value"}, ttl_seconds=60)
    assert set(await redis_client.keys("*")) == {b"schemas", b"schema:my.api"}

    schemas = await redis_client.smembers("schemas")
    assert schemas == [b"my.api"]

    my_api_schema = await redis_client.get("schema:my.api")
    assert json.loads(my_api_schema) == {"key": "value"}

    ttl = await redis_client.ttl("schema:my.api")
    assert 59 <= ttl <= 60


@pytest.mark.asyncio
async def test_store_no_ttl(redis_schema_transport: RedisSchemaTransport, redis_client):
    await redis_schema_transport.store("my.api", {"key": "value"}, ttl_seconds=None)
    ttl = await redis_client.ttl("schema:my.api")
    assert ttl == -1


@pytest.mark.asyncio
async def test_load(redis_schema_transport: RedisSchemaTransport, redis_client):
    await redis_client.sadd("schemas", "my.api", "old.api")
    await redis_client.set("schema:my.api", json.dumps({"key": "value"}))

    schemas = await redis_schema_transport.load()

    assert schemas == {"my.api": {"key": "value"}}


@pytest.mark.asyncio
async def test_load(redis_schema_transport: RedisSchemaTransport, redis_client):
    await redis_client.sadd("schemas", "my.api", "old.api")
    await redis_client.set("schema:my.api", json.dumps({"key": "value"}))

    schemas = await redis_schema_transport.load()

    assert schemas == {"my.api": {"key": "value"}}


@pytest.mark.asyncio
async def test_load_no_apis(redis_schema_transport: RedisSchemaTransport, redis_client):
    schemas = await redis_schema_transport.load()
    assert schemas == {}


@pytest.mark.asyncio
async def test_from_config(redis_client):
    await redis_client.select(5)
    host, port = redis_client.address
    transport = RedisSchemaTransport.from_config(
        config=None, url=f"redis://127.0.0.1:{port}/5", connection_parameters=dict(maxsize=3)
    )
    with await transport.connection_manager() as transport_client:
        assert transport_client.connection.address == ("127.0.0.1", port)
        assert transport_client.connection.db == 5
        await transport_client.set("x", 1)
        assert await redis_client.get("x")

    assert transport._redis_pool.connection.maxsize == 3
