import asyncio
from datetime import datetime

import pytest

from lightbus.config import Config
from lightbus.message import EventMessage
from lightbus.serializers import ByFieldMessageSerializer, ByFieldMessageDeserializer, BlobMessageSerializer, \
    BlobMessageDeserializer
from lightbus.transports.redis import RedisEventTransport


pytestmark = pytest.mark.unit


@pytest.mark.run_loop
async def test_connection_manager(redis_event_transport):
    """Does get_redis() provide a working redis connection"""
    connection_manager = await redis_event_transport.connection_manager()
    with await connection_manager as redis:
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
        b'api_name': b'my.api',
        b'event_name': b'my_event',
        b':field': b'"value"',
    }


@pytest.mark.run_loop
async def test_consume_events(redis_event_transport: RedisEventTransport, redis_client, dummy_api):
    async def co_enqeue():
        await asyncio.sleep(0.1)
        return await redis_client.xadd('my.dummy.my_event:stream', fields={
            b'api_name': b'my.dummy',
            b'event_name': b'my_event',
            b':field': b'"value"',
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
            b'api_name': b'my.dummy',
            b'event_name': b'my_event',
            b':field': b'"1"',
        },
        message_id='1515000001000-0',
    )
    await redis_client.xadd(
        'my.dummy.my_event:stream',
        fields={
            b'api_name': b'my.dummy',
            b'event_name': b'my_event',
            b':field': b'"2"',
        },
        message_id='1515000002000-0',
    )
    await redis_client.xadd(
        'my.dummy.my_event:stream',
        fields={
            b'api_name': b'my.dummy',
            b'event_name': b'my_event',
            b':field': b'"3"',
        },
        message_id='1515000003000-0',
    )

    consumer = redis_event_transport.consume([('my.dummy', 'my_event')], {}, since='1515000001500-0', forever=False)

    yields = []

    async def co():
        async for m in consumer:
            yields.append(m)
    asyncio.ensure_future(co())
    await asyncio.sleep(0.1)

    assert len(yields) == 4
    assert yields[0].kwargs['field'] == '2'
    assert yields[1] is True
    assert yields[2].kwargs['field'] == '3'
    assert yields[3] is True


@pytest.mark.run_loop
async def test_consume_events_since_datetime(redis_event_transport: RedisEventTransport, redis_client, dummy_api):
    await redis_client.xadd(
        'my.dummy.my_event:stream',
        fields={
            b'api_name': b'my.dummy',
            b'event_name': b'my_event',
            b':field': b'"1"',
        },
        message_id='1515000001000-0',
    )
    await redis_client.xadd(
        'my.dummy.my_event:stream',
        fields={
            b'api_name': b'my.dummy',
            b'event_name': b'my_event',
            b':field': b'"2"',
        },
        message_id='1515000002000-0',
    )
    await redis_client.xadd(
        'my.dummy.my_event:stream',
        fields={
            b'api_name': b'my.dummy',
            b'event_name': b'my_event',
            b':field': b'"3"',
        },
        message_id='1515000003000-0',
    )

    # 1515000001500-0 -> 2018-01-03T17:20:01.500Z
    since_datetime = datetime(2018, 1, 3, 17, 20, 1, 500)
    consumer = redis_event_transport.consume([('my.dummy', 'my_event')], {}, since=since_datetime, forever=False)

    yields = []
    
    async def co():
        async for m in consumer:
            yields.append(m)
    asyncio.ensure_future(co())
    await asyncio.sleep(0.1)

    assert len(yields) == 4
    assert yields[0].kwargs['field'] == '2'
    assert yields[1] is True
    assert yields[2].kwargs['field'] == '3'
    assert yields[3] is True


@pytest.mark.run_loop
async def test_from_config(redis_client):
    await redis_client.select(5)
    host, port = redis_client.address
    transport = RedisEventTransport.from_config(
        config=Config.load_dict({}),
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
