import asyncio

import pytest

from lightbus.message import EventMessage
from lightbus.transports.redis import RedisEventTransport


@pytest.mark.run_loop
async def test_get_redis(redis_event_transport):
    """Does get_redis() provide a working redis connection"""
    redis = await redis_event_transport.get_redis()
    assert await redis.info()
    redis.close()


@pytest.mark.run_loop
async def test_send_event(redis_event_transport: RedisEventTransport, redis_client):
    await redis_event_transport.send_event(EventMessage(
        api_name='my.api',
        event_name='my_event',
        kwargs={'field': 'value'},
    ))
    messages = await redis_client.xrange('my.api.my_event:stream')
    assert len(messages) == 1
    assert messages[0][1] == {
        b'api_name': b'my.api',
        b'event_name': b'my_event',
        b'kw:field': b'value',
    }


@pytest.mark.run_loop
async def test_consume_events(redis_event_transport: RedisEventTransport, redis_client, dummy_api):
    async def co_enqeue():
        await asyncio.sleep(0.1)
        return await redis_client.xadd('my.dummy.my_event:stream', fields={
            b'api_name': b'my.dummy',
            b'event_name': b'my_event',
            b'kw:field': b'value',
        })

    async def co_consume():
        await redis_event_transport.start_listening_for(dummy_api, 'my_event')
        return await redis_event_transport.consume_events()

    enqueue_result, messages = await asyncio.gather(co_enqeue(), co_consume())
    message = messages[0]
    assert message.api_name == 'my.dummy'
    assert message.event_name == 'my_event'
    assert message.kwargs == {'field': b'value'}
