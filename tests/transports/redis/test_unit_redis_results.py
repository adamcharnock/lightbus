import json
from uuid import UUID

import pytest

from lightbus.message import RpcMessage, ResultMessage
from lightbus.serializers import ByFieldMessageSerializer, ByFieldMessageDeserializer
from lightbus import RedisResultTransport

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_connection_manager(redis_result_transport):
    """Does get_redis() provide a working redis connection"""
    connection_manager = await redis_result_transport.connection_manager()
    with await connection_manager as redis:
        assert await redis.info()


@pytest.mark.asyncio
async def test_get_return_path(redis_result_transport: RedisResultTransport):
    return_path = await redis_result_transport.get_return_path(
        RpcMessage(
            api_name="my.api",
            procedure_name="my_proc",
            kwargs={"field": "value"},
            return_path="abc",
        )
    )
    assert return_path.startswith("redis+key://my.api.my_proc:result:")
    result_uuid = return_path.split(":")[-1]
    assert UUID(hex=result_uuid)


@pytest.mark.asyncio
async def test_send_result(redis_result_transport: RedisResultTransport, redis_client):
    await redis_result_transport.send_result(
        rpc_message=RpcMessage(
            id="123abc",
            api_name="my.api",
            procedure_name="my_proc",
            kwargs={"field": "value"},
            return_path="abc",
        ),
        result_message=ResultMessage(
            api_name="my.api",
            procedure_name="my_proc",
            id="345",
            rpc_message_id="123abc",
            result="All done! 😎",
        ),
        return_path="redis+key://my.api.my_proc:result:e1821498-e57c-11e7-af9d-7831c1c3936e",
    )
    assert await redis_client.keys("*") == [
        b"my.api.my_proc:result:e1821498-e57c-11e7-af9d-7831c1c3936e"
    ]

    result = await redis_client.lpop("my.api.my_proc:result:e1821498-e57c-11e7-af9d-7831c1c3936e")
    assert json.loads(result) == {
        "metadata": {"error": False, "rpc_message_id": "123abc", "id": "345"},
        "kwargs": {"result": "All done! 😎"},
    }


@pytest.mark.asyncio
async def test_receive_result(redis_result_transport: RedisResultTransport, redis_client):

    redis_client.lpush(
        key="my.api.my_proc:result:e1821498-e57c-11e7-af9d-7831c1c3936e",
        value=json.dumps(
            {
                "metadata": {"rpc_message_id": "123abc", "error": False, "id": "123"},
                "kwargs": {"result": "All done! 😎"},
            }
        ),
    )

    result_message = await redis_result_transport.receive_result(
        rpc_message=RpcMessage(
            id="123abc",
            api_name="my.api",
            procedure_name="my_proc",
            kwargs={"field": "value"},
            return_path="abc",
        ),
        return_path="redis+key://my.api.my_proc:result:e1821498-e57c-11e7-af9d-7831c1c3936e",
        options={},
    )
    assert result_message.result == "All done! 😎"
    assert result_message.rpc_message_id == "123abc"
    assert result_message.id == "123"
    assert result_message.error == False


@pytest.mark.asyncio
async def test_from_config(redis_client):
    await redis_client.select(5)
    host, port = redis_client.address
    transport = RedisResultTransport.from_config(
        url=f"redis://127.0.0.1:{port}/5",
        connection_parameters=dict(maxsize=123),
        # Non default serializers, event though they wouldn't make sense in this context
        serializer="lightbus.serializers.ByFieldMessageSerializer",
        deserializer="lightbus.serializers.ByFieldMessageDeserializer",
        config=None,
    )
    with await transport.connection_manager() as transport_client:
        assert transport_client.connection.address == ("127.0.0.1", port)
        assert transport_client.connection.db == 5
        await transport_client.set("x", 1)
        assert await redis_client.get("x")

    assert transport._redis_pool.connection.maxsize == 123
    assert isinstance(transport.serializer, ByFieldMessageSerializer)
    assert isinstance(transport.deserializer, ByFieldMessageDeserializer)
    await transport.close()
