import asyncio

import pytest

from lightbus.message import RpcMessage
from lightbus.transports.redis import RedisRpcTransport


@pytest.fixture
def redis_rpc_transport(create_redis_client, server, loop):
    """Get a redis transport backed by a running redis server."""
    return RedisRpcTransport(redis=loop.run_until_complete(
        create_redis_client(server.tcp_address, loop=loop)
    ))


@pytest.mark.run_loop
async def test_get_redis(redis_rpc_transport, loop):
    """Does get_redis() provide a working redis connection"""
    redis = await redis_rpc_transport.get_redis()
    assert await redis.info()
    redis.close()


@pytest.mark.run_loop
async def test_call_rpc(redis_rpc_transport, loop):
    """Does call_rpc() add a message to a stream"""
    rpc_message = RpcMessage(
        api_name='my.api',
        procedure_name='my_proc',
        kwargs={'field': 'value'},
        return_path='abc',
    )
    await redis_rpc_transport.call_rpc(rpc_message)
    redis = await redis_rpc_transport.get_redis()
    assert await redis.keys('*') == [b'my.api:stream']

    messages = await redis.xrange('my.api:stream')
    assert len(messages) == 1
    assert messages[0][1] == {
        b'api_name': b'my.api',
        b'procedure_name': b'my_proc',
        b'kw:field': b'value',
        b'return_path': b'abc'
    }

    redis.close()


@pytest.mark.run_loop
async def test_consume_rpcs(redis_client, redis_rpc_transport, dummy_api):

    async def co_enqeue():
        await asyncio.sleep(0.01)
        return await redis_client.xadd('dummy.api:stream', fields={
            b'api_name': b'my.api',
            b'procedure_name': b'my_proc',
            b'kw:field': b'value',
            b'return_path': b'abc'
        })

    async def co_consume():
        return await redis_rpc_transport.consume_rpcs(apis=[dummy_api])

    enqueue_result, messages = await asyncio.gather(co_enqeue(), co_consume())
    message = messages[0]
    assert message.api_name == 'my.api'
    assert message.procedure_name == 'my_proc'
    assert message.kwargs == {'field': b'value'}
    assert message.return_path == 'abc'
