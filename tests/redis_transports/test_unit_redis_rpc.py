import asyncio
import json
import logging

import pytest

from lightbus import RedisRpcTransport
from lightbus.message import RpcMessage
from lightbus.serializers import BlobMessageSerializer, BlobMessageDeserializer
from lightbus.utilities.async_tools import cancel

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_connection_manager(redis_rpc_transport):
    """Does get_redis() provide a working redis connection"""
    connection_manager = await redis_rpc_transport.connection_manager()
    with await connection_manager as redis:
        assert await redis.info()


@pytest.mark.asyncio
async def test_call_rpc(redis_rpc_transport, redis_client):
    """Does call_rpc() add a message to a stream"""
    rpc_message = RpcMessage(
        id="123abc",
        api_name="my.api",
        procedure_name="my_proc",
        kwargs={"field": "value"},
        return_path="abc",
    )
    await redis_rpc_transport.call_rpc(rpc_message, options={})
    assert set(await redis_client.keys("*")) == {b"my.api:rpc_queue", b"rpc_expiry_key:123abc"}

    messages = await redis_client.lrange("my.api:rpc_queue", start=0, stop=100)
    assert len(messages) == 1
    message = json.loads(messages[0])
    assert message == {
        "metadata": {
            "id": "123abc",
            "api_name": "my.api",
            "procedure_name": "my_proc",
            "return_path": "abc",
        },
        "kwargs": {"field": "value"},
    }
    assert await redis_client.exists("rpc_expiry_key:123abc")
    assert await redis_client.ttl("rpc_expiry_key:123abc") == redis_rpc_transport.rpc_timeout


@pytest.mark.asyncio
async def test_consume_rpcs_no_expiry_key(redis_client, redis_rpc_transport, dummy_api):
    """Does call_rpc() add a message to a stream, but where the expiry key is missing

    Transport should assume that the RPC call has timed out and therefore not serve it.
    """

    async def co_enqeue():
        await asyncio.sleep(0.01)
        # NOT SETTING rpc_expiry_key:123abc
        return await redis_client.rpush(
            "my.dummy:rpc_queue",
            value=json.dumps(
                {
                    "metadata": {
                        "id": "123abc",
                        "api_name": "my.api",
                        "procedure_name": "my_proc",
                        "return_path": "abc",
                    },
                    "kwargs": {"field": "value"},
                }
            ),
        )

    async def co_consume():
        return await redis_rpc_transport.consume_rpcs(apis=[dummy_api])

    enqueue_result, messages = await asyncio.gather(co_enqeue(), co_consume())
    assert not messages


@pytest.mark.asyncio
async def test_consume_rpcs(redis_client, redis_rpc_transport, dummy_api):
    async def co_enqeue():
        await asyncio.sleep(0.01)
        await redis_client.set("rpc_expiry_key:123abc", 1)
        return await redis_client.rpush(
            "my.dummy:rpc_queue",
            value=json.dumps(
                {
                    "metadata": {
                        "id": "123abc",
                        "api_name": "my.api",
                        "procedure_name": "my_proc",
                        "return_path": "abc",
                    },
                    "kwargs": {"field": "value"},
                }
            ),
        )

    async def co_consume():
        return await redis_rpc_transport.consume_rpcs(apis=[dummy_api])

    enqueue_result, messages = await asyncio.gather(co_enqeue(), co_consume())
    message = messages[0]
    assert message.id == "123abc"
    assert message.api_name == "my.api"
    assert message.procedure_name == "my_proc"
    assert message.kwargs == {"field": "value"}
    assert message.return_path == "abc"


@pytest.mark.asyncio
async def test_from_config(redis_client):
    await redis_client.select(5)
    host, port = redis_client.address
    transport = RedisRpcTransport.from_config(
        config=None,
        url=f"redis://127.0.0.1:{port}/5",
        connection_parameters=dict(maxsize=123),
        batch_size=123,
        # Non default serializers, event though they wouldn't make sense in this context
        serializer="lightbus.serializers.BlobMessageSerializer",
        deserializer="lightbus.serializers.BlobMessageDeserializer",
    )
    with await transport.connection_manager() as transport_client:
        assert transport_client.connection.address == ("127.0.0.1", port)
        assert transport_client.connection.db == 5
        await transport_client.set("x", 1)
        assert await redis_client.get("x")

    assert transport._redis_pool.connection.maxsize == 123
    assert isinstance(transport.serializer, BlobMessageSerializer)
    assert isinstance(transport.deserializer, BlobMessageDeserializer)


@pytest.mark.asyncio
async def test_consume_rpcs_only_once(redis_client, dummy_api, redis_pool):
    """Ensure that an RPC call gets consumed only once even with multiple listeners"""
    message_count = 0

    transport1 = RedisRpcTransport(redis_pool=redis_pool)
    transport2 = RedisRpcTransport(redis_pool=redis_pool)

    async def co_consume(transport):
        nonlocal message_count
        messages = await transport.consume_rpcs(apis=[dummy_api])
        message_count += len(messages)

    consumer1 = asyncio.ensure_future(co_consume(transport1))
    consumer2 = asyncio.ensure_future(co_consume(transport2))
    await asyncio.sleep(0.1)

    await redis_client.set("rpc_expiry_key:123abc", 1)
    await redis_client.rpush(
        "my.dummy:rpc_queue",
        json.dumps(
            {
                "metadata": {
                    "id": "123abc",
                    "api_name": "my.api",
                    "procedure_name": "my_proc",
                    "return_path": "abc",
                },
                "kwargs": {"field": "value"},
            }
        ),
    )
    await asyncio.sleep(0.1)

    await cancel(consumer1, consumer2)
    assert message_count == 1

    assert not await redis_client.exists("rpc_expiry_key:123abc")


@pytest.mark.asyncio
async def test_reconnect_upon_call_rpc(redis_rpc_transport, redis_client):
    """Does call_rpc() add a message to a stream"""
    # Kill the rpc transport's connection
    await redis_client.execute(b"CLIENT", b"KILL", b"TYPE", b"NORMAL")

    # Now send a message and ensure it does so without complaint
    rpc_message = RpcMessage(
        id="123abc",
        api_name="my.api",
        procedure_name="my_proc",
        kwargs={"field": "value"},
        return_path="abc",
    )
    await redis_rpc_transport.call_rpc(rpc_message, options={})
    assert set(await redis_client.keys("*")) == {b"my.api:rpc_queue", b"rpc_expiry_key:123abc"}

    messages = await redis_client.lrange("my.api:rpc_queue", start=0, stop=100)
    assert len(messages) == 1


@pytest.mark.asyncio
async def test_reconnect_upon_consume_rpcs(loop, redis_client, redis_rpc_transport, dummy_api):
    redis_rpc_transport.consumption_restart_delay = 0.0001

    async def co_enqeue():
        while True:
            await asyncio.sleep(0.01)
            await redis_client.set("rpc_expiry_key:123abc", 1)
            await redis_client.rpush(
                "my.dummy:rpc_queue",
                value=json.dumps(
                    {
                        "metadata": {
                            "id": "123abc",
                            "api_name": "my.api",
                            "procedure_name": "my_proc",
                            "return_path": "abc",
                        },
                        "kwargs": {"field": "value"},
                    }
                ),
            )

    total_messages = 0

    async def co_consume():
        nonlocal total_messages
        while True:
            messages = await redis_rpc_transport.consume_rpcs(apis=[dummy_api])
            total_messages += len(messages)

    enque_task = asyncio.ensure_future(co_enqeue())
    consume_task = asyncio.ensure_future(co_consume())

    await asyncio.sleep(0.2)
    assert total_messages > 0
    await redis_client.execute(b"CLIENT", b"KILL", b"TYPE", b"NORMAL")
    total_messages = 0
    await asyncio.sleep(0.2)
    assert total_messages > 0

    await cancel(enque_task, consume_task)
