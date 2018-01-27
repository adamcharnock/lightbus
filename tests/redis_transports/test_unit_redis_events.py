import asyncio
from datetime import datetime

import pytest

from lightbus.message import EventMessage
from lightbus.transports.redis import RedisEventTransport


pytestmark = pytest.mark.unit


@pytest.mark.run_loop
async def test_get_redis(redis_event_transport):
    """Does get_redis() provide a working redis connection"""
    pool = await redis_event_transport.get_redis_pool()
    with await pool as redis:
        assert await redis.info()


@pytest.mark.run_loop
async def test_send_event(redis_event_transport: RedisEventTransport, redis_client):
    await redis_event_transport.send_event(EventMessage(
        api_name='my.api',
        event_name='my_event',
        kwargs={'field': 'value'},
    ), options={})
    messages = await redis_client.xrange('my.api.my_event:stream')
    assert len(messages) == 1
    assert messages[0][1] == {
        b'api_name': b'"my.api"',
        b'event_name': b'"my_event"',
        b'kw:field': b'"value"',
    }


@pytest.mark.run_loop
async def test_consume_events(redis_event_transport: RedisEventTransport, redis_client, dummy_api):
    async def co_enqeue():
        await asyncio.sleep(0.1)
        return await redis_client.xadd('my.dummy.my_event:stream', fields={
            b'api_name': b'"my.dummy"',
            b'event_name': b'"my_event"',
            b'kw:field': b'"value"',
        })

    async def co_consume():
        async for message_ in redis_event_transport.consume([('my.dummy', 'my_event')], {}):
            return message_

    enqueue_result, message = await asyncio.gather(co_enqeue(), co_consume())
    assert message.api_name == 'my.dummy'
    assert message.event_name == 'my_event'
    assert message.kwargs == {'field': 'value'}


@pytest.mark.run_loop
async def test_consume_events_since_id(redis_event_transport: RedisEventTransport, redis_client, dummy_api):
    await redis_client.xadd(
        'my.dummy.my_event:stream',
        fields={
            b'api_name': b'"my.dummy"',
            b'event_name': b'"my_event"',
            b'kw:field': b'"1"',
        },
        message_id='1515000001000-0',
    )
    await redis_client.xadd(
        'my.dummy.my_event:stream',
        fields={
            b'api_name': b'"my.dummy"',
            b'event_name': b'"my_event"',
            b'kw:field': b'"2"',
        },
        message_id='1515000002000-0',
    )
    await redis_client.xadd(
        'my.dummy.my_event:stream',
        fields={
            b'api_name': b'"my.dummy"',
            b'event_name': b'"my_event"',
            b'kw:field': b'"3"',
        },
        message_id='1515000003000-0',
    )

    consumer = redis_event_transport.consume([('my.dummy', 'my_event')], {}, since='1515000001500-0', forever=False)
    messages = [m async for m in consumer]

    assert len(messages) == 2
    assert messages[0].kwargs['field'] == '2'
    assert messages[1].kwargs['field'] == '3'


@pytest.mark.run_loop
async def test_consume_events_since_datetime(redis_event_transport: RedisEventTransport, redis_client, dummy_api):
    await redis_client.xadd(
        'my.dummy.my_event:stream',
        fields={
            b'api_name': b'"my.dummy"',
            b'event_name': b'"my_event"',
            b'kw:field': b'"1"',
        },
        message_id='1515000001000-0',
    )
    await redis_client.xadd(
        'my.dummy.my_event:stream',
        fields={
            b'api_name': b'"my.dummy"',
            b'event_name': b'"my_event"',
            b'kw:field': b'"2"',
        },
        message_id='1515000002000-0',
    )
    await redis_client.xadd(
        'my.dummy.my_event:stream',
        fields={
            b'api_name': b'"my.dummy"',
            b'event_name': b'"my_event"',
            b'kw:field': b'"3"',
        },
        message_id='1515000003000-0',
    )

    # 1515000001500-0 -> 2018-01-03T17:20:01.500Z
    since_datetime = datetime(2018, 1, 3, 17, 20, 1, 500)
    consumer = redis_event_transport.consume([('my.dummy', 'my_event')], {}, since=since_datetime, forever=False)
    messages = [m async for m in consumer]

    assert len(messages) == 2
    assert messages[0].kwargs['field'] == '2'
    assert messages[1].kwargs['field'] == '3'
