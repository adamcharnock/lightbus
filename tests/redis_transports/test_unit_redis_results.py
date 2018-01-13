import json
from uuid import UUID

import pytest

from lightbus.message import RpcMessage, ResultMessage
from lightbus.transports.redis import RedisResultTransport


pytestmark = pytest.mark.unit


@pytest.mark.run_loop
async def test_get_redis(redis_result_transport: RedisResultTransport):
    """Does get_redis() provide a working redis connection"""
    pool = await redis_result_transport.get_redis_pool()
    with await pool as redis:
        assert await redis.info()


@pytest.mark.run_loop
async def test_get_return_path(redis_result_transport: RedisResultTransport):
    return_path = redis_result_transport.get_return_path(RpcMessage(
        api_name='my.api',
        procedure_name='my_proc',
        kwargs={'field': 'value'},
        return_path='abc',
    ))
    assert return_path.startswith('redis+key://my.api.my_proc:result:')
    result_uuid = return_path.split(':')[-1]
    assert UUID(hex=result_uuid)


@pytest.mark.run_loop
async def test_send_result(redis_result_transport: RedisResultTransport, redis_client):
    await redis_result_transport.send_result(
        rpc_message=RpcMessage(
            rpc_id='123abc',
            api_name='my.api',
            procedure_name='my_proc',
            kwargs={'field': 'value'},
            return_path='abc',
        ),
        result_message=ResultMessage(
            rpc_id='123abc',
            result='All done! 😎',
        ),
        return_path='redis+key://my.api.my_proc:result:e1821498-e57c-11e7-af9d-7831c1c3936e',
    )
    assert await redis_client.keys('*') == [b'my.api.my_proc:result:e1821498-e57c-11e7-af9d-7831c1c3936e']

    result = await redis_client.lpop('my.api.my_proc:result:e1821498-e57c-11e7-af9d-7831c1c3936e')
    assert json.loads(result) == {
        'error': False,
        'rpc_id': '123abc',
        'result': 'All done! 😎',
    }


@pytest.mark.run_loop
async def test_receive_result(redis_result_transport: RedisResultTransport, redis_client):

    redis_client.lpush(
        key='my.api.my_proc:result:e1821498-e57c-11e7-af9d-7831c1c3936e',
        value=json.dumps({
            'result': 'All done! 😎',
            'rpc_id': '123abc',
            'error': False,
        }),
    )

    result_message = await redis_result_transport.receive_result(
        rpc_message=RpcMessage(
            rpc_id='123abc',
            api_name='my.api',
            procedure_name='my_proc',
            kwargs={'field': 'value'},
            return_path='abc',
        ),
        return_path='redis+key://my.api.my_proc:result:e1821498-e57c-11e7-af9d-7831c1c3936e',
    )
    assert result_message.result == 'All done! 😎'
    assert result_message.rpc_id == '123abc'
    assert result_message.error == False
