import asyncio

import pytest

from lightbus.message import RpcMessage


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
        b'rpc_id': b'"123abc"',
        b'api_name': b'"my.api"',
        b'procedure_name': b'"my_proc"',
        b'kw:field': b'"value"',
        b'return_path': b'"abc"',
    }


@pytest.mark.run_loop
async def test_consume_rpcs(redis_client, redis_rpc_transport, dummy_api):

    async def co_enqeue():
        await asyncio.sleep(0.01)
        return await redis_client.xadd('my.dummy:stream', fields={
            b'rpc_id': b'"123abc"',
            b'api_name': b'"my.api"',
            b'procedure_name': b'"my_proc"',
            b'kw:field': b'"value"',
            b'return_path': b'"abc"',
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
