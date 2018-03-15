import asyncio

import pytest

from lightbus import RedisRpcTransport
from lightbus.message import RpcMessage
from lightbus.serializers import BlobMessageSerializer, BlobMessageDeserializer

pytestmark = pytest.mark.unit


@pytest.mark.run_loop
async def test_connection_manager(redis_rpc_transport):
    """Does get_redis() provide a working redis connection"""
    connection_manager = await redis_rpc_transport.connection_manager()
    with await connection_manager as redis:
        assert await redis.info()


@pytest.mark.run_loop
async def test_call_rpc(redis_rpc_transport, redis_client):
    """Does call_rpc() add a message to a stream"""
    rpc_message = RpcMessage(
        rpc_id='123abc',
        api_name='my.api',
        procedure_name='my_proc',
        kwargs={'field': 'value'},
        return_path='abc',
    )
    await redis_rpc_transport.call_rpc(rpc_message, options={})
    assert await redis_client.keys('*') == [b'my.api:stream']

    messages = await redis_client.xrange('my.api:stream')
    assert len(messages) == 1
    assert messages[0][1] == {
        b'rpc_id': b'123abc',
        b'api_name': b'my.api',
        b'procedure_name': b'my_proc',
        b':field': b'"value"',
        b'return_path': b'abc',
    }


@pytest.mark.run_loop
async def test_consume_rpcs(redis_client, redis_rpc_transport, dummy_api):

    async def co_enqeue():
        await asyncio.sleep(0.01)
        return await redis_client.xadd('my.dummy:stream', fields={
            'rpc_id': '123abc',
            'api_name': 'my.api',
            'procedure_name': 'my_proc',
            'return_path': 'abc',
            ':field': '"value"',
        })

    async def co_consume():
        return await redis_rpc_transport.consume_rpcs(apis=[dummy_api])

    enqueue_result, messages = await asyncio.gather(co_enqeue(), co_consume())
    message = messages[0]
    assert message.rpc_id == '123abc'
    assert message.api_name == 'my.api'
    assert message.procedure_name == 'my_proc'
    assert message.kwargs == {'field': 'value'}
    assert message.return_path == 'abc'


@pytest.mark.run_loop
async def test_from_config(redis_client):
    await redis_client.select(5)
    host, port = redis_client.address
    transport = RedisRpcTransport.from_config(
        url=f'redis://127.0.0.1:{port}/5',
        connection_parameters=dict(maxsize=123),
        batch_size=123,
        # Non default serializers, event though they wouldn't make sense in this context
        serializer='lightbus.serializers.BlobMessageSerializer',
        deserializer='lightbus.serializers.BlobMessageDeserializer',
    )
    with await transport.connection_manager() as transport_client:
        assert transport_client.connection.address == ('127.0.0.1', port)
        assert transport_client.connection.db == 5
        await transport_client.set('x', 1)
        assert await redis_client.get('x')

    assert transport._redis_pool.connection.maxsize == 123
    assert isinstance(transport.serializer, BlobMessageSerializer)
    assert isinstance(transport.deserializer, BlobMessageDeserializer)
