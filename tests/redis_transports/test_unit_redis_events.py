import asyncio
import logging
from datetime import datetime

import pytest

from lightbus.config import Config
from lightbus.message import EventMessage
from lightbus.serializers import (
    ByFieldMessageSerializer,
    ByFieldMessageDeserializer,
    BlobMessageSerializer,
    BlobMessageDeserializer,
)
from lightbus.transports.redis import RedisEventTransport, StreamUse
from lightbus.utilities.async import cancel

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_connection_manager(redis_event_transport):
    """Does get_redis() provide a working redis connection"""
    connection_manager = await redis_event_transport.connection_manager()
    with await connection_manager as redis:
        assert await redis.info()


@pytest.mark.asyncio
async def test_send_event(redis_event_transport: RedisEventTransport, redis_client):
    await redis_event_transport.send_event(
        EventMessage(api_name="my.api", event_name="my_event", id="123", kwargs={"field": "value"}),
        options={},
    )
    messages = await redis_client.xrange("my.api.my_event:stream")
    assert len(messages) == 1
    assert messages[0][1] == {
        b"api_name": b"my.api",
        b"event_name": b"my_event",
        b"id": b"123",
        b"version": b"1",
        b":field": b'"value"',
    }


@pytest.mark.asyncio
async def test_send_event_per_api_stream(redis_event_transport: RedisEventTransport, redis_client):
    redis_event_transport.stream_use = StreamUse.PER_API
    await redis_event_transport.send_event(
        EventMessage(api_name="my.api", event_name="my_event", kwargs={"field": "value"}, id="123"),
        options={},
    )
    messages = await redis_client.xrange("my.api.*:stream")
    assert len(messages) == 1
    assert messages[0][1] == {
        b"api_name": b"my.api",
        b"event_name": b"my_event",
        b"id": b"123",
        b"version": b"1",
        b":field": b'"value"',
    }


@pytest.mark.asyncio
async def test_consume_events(
    loop, redis_event_transport: RedisEventTransport, redis_client, dummy_api
):

    async def co_enqeue():
        await asyncio.sleep(0.1)
        return await redis_client.xadd(
            "my.dummy.my_event:stream",
            fields={
                b"api_name": b"my.dummy",
                b"event_name": b"my_event",
                b"id": b"123",
                b"version": b"1",
                b":field": b'"value"',
            },
        )

    async def co_consume():
        async for message_ in redis_event_transport.consume([("my.dummy", "my_event")], {}):
            return message_

    enqueue_result, message = await asyncio.gather(co_enqeue(), co_consume())
    assert message.api_name == "my.dummy"
    assert message.event_name == "my_event"
    assert message.kwargs == {"field": "value"}
    assert message.native_id
    assert type(message.native_id) == str


@pytest.mark.asyncio
async def test_consume_events_multiple_consumers(
    loop, redis_event_transport: RedisEventTransport, redis_client, dummy_api
):
    messages = []

    async def co_consume(name):
        async for message_ in redis_event_transport.consume([("my.dummy", "my_event")], {}):
            messages.append((name, message_))

    task1 = asyncio.ensure_future(co_consume("task1"))
    task2 = asyncio.ensure_future(co_consume("task2"))

    await asyncio.sleep(0.1)
    await redis_client.xadd(
        "my.dummy.my_event:stream",
        fields={
            b"api_name": b"my.dummy",
            b"event_name": b"my_event",
            b"id": b"123",
            b"version": b"1",
            b":field": b'"value"',
        },
    )
    await asyncio.sleep(0.1)

    # Two messages, to dummy values which indicate events have been acked
    assert len(messages) == 4

    await cancel(task1, task2)


@pytest.mark.asyncio
async def test_consume_events_multiple_consumers_one_group(
    loop, redis_pool, redis_client, dummy_api
):
    messages = []

    async def co_consume(consumer_number):
        event_transport = RedisEventTransport(
            redis_pool=redis_pool,
            consumer_group_prefix="test_cg",
            consumer_name=f"test_consumer{consumer_number}",
            stream_use=StreamUse.PER_EVENT,
        )
        consumer = event_transport.consume(
            listen_for=[("my.dummy", "my_event")], context={}, consumer_group="single_group"
        )
        async for message_ in consumer:
            messages.append(message_)

    task1 = asyncio.ensure_future(co_consume(1))
    task2 = asyncio.ensure_future(co_consume(2))
    await asyncio.sleep(0.1)

    await redis_client.xadd(
        "my.dummy.my_event:stream",
        fields={
            b"api_name": b"my.dummy",
            b"event_name": b"my_event",
            b"id": b"123",
            b"version": b"1",
            b":field": b'"value"',
        },
    )
    await asyncio.sleep(0.1)
    await cancel(task1, task2)

    # One message, one dummy value which indicate events have been acked
    assert len(messages) == 2


@pytest.mark.asyncio
async def test_consume_events_since_id(
    loop, redis_event_transport: RedisEventTransport, redis_client, dummy_api
):
    await redis_client.xadd(
        "my.dummy.my_event:stream",
        fields={
            b"api_name": b"my.dummy",
            b"event_name": b"my_event",
            b"id": b"123",
            b"version": b"1",
            b":field": b'"1"',
        },
        message_id="1515000001000-0",
    )
    await redis_client.xadd(
        "my.dummy.my_event:stream",
        fields={
            b"api_name": b"my.dummy",
            b"event_name": b"my_event",
            b"id": b"123",
            b"version": b"1",
            b":field": b'"2"',
        },
        message_id="1515000002000-0",
    )
    await redis_client.xadd(
        "my.dummy.my_event:stream",
        fields={
            b"api_name": b"my.dummy",
            b"event_name": b"my_event",
            b"id": b"123",
            b"version": b"1",
            b":field": b'"3"',
        },
        message_id="1515000003000-0",
    )

    consumer = redis_event_transport.consume(
        [("my.dummy", "my_event")], {}, since="1515000001500-0", forever=False
    )

    yields = []

    async def co():
        async for m in consumer:
            yields.append(m)

    asyncio.ensure_future(co())
    await asyncio.sleep(0.1)

    messages_ids = [m.native_id for m in yields if isinstance(m, EventMessage)]

    assert len(messages_ids) == 2
    assert len(yields) == 4
    assert yields[0].kwargs["field"] == "2"
    assert yields[1] is True
    assert yields[2].kwargs["field"] == "3"
    assert yields[3] is True


@pytest.mark.asyncio
async def test_consume_events_since_datetime(
    loop, redis_event_transport: RedisEventTransport, redis_client, dummy_api
):
    await redis_client.xadd(
        "my.dummy.my_event:stream",
        fields={
            b"api_name": b"my.dummy",
            b"event_name": b"my_event",
            b"id": b"123",
            b"version": b"1",
            b":field": b'"1"',
        },
        message_id="1515000001000-0",
    )
    await redis_client.xadd(
        "my.dummy.my_event:stream",
        fields={
            b"api_name": b"my.dummy",
            b"event_name": b"my_event",
            b"id": b"123",
            b"version": b"1",
            b":field": b'"2"',
        },
        message_id="1515000002000-0",
    )
    await redis_client.xadd(
        "my.dummy.my_event:stream",
        fields={
            b"api_name": b"my.dummy",
            b"event_name": b"my_event",
            b"id": b"123",
            b"version": b"1",
            b":field": b'"3"',
        },
        message_id="1515000003000-0",
    )

    # 1515000001500-0 -> 2018-01-03T17:20:01.500Z
    since_datetime = datetime(2018, 1, 3, 17, 20, 1, 500)
    consumer = redis_event_transport.consume(
        [("my.dummy", "my_event")], {}, since=since_datetime, forever=False
    )

    yields = []

    async def co():
        async for m in consumer:
            yields.append(m)

    asyncio.ensure_future(co())
    await asyncio.sleep(0.1)

    assert len(yields) == 4
    assert yields[0].kwargs["field"] == "2"
    assert yields[1] is True
    assert yields[2].kwargs["field"] == "3"
    assert yields[3] is True


@pytest.mark.asyncio
async def test_from_config(redis_client):
    await redis_client.select(5)
    host, port = redis_client.address
    transport = RedisEventTransport.from_config(
        config=Config.load_dict({}),
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

    assert transport._local.redis_pool.connection.maxsize == 123
    assert isinstance(transport.serializer, BlobMessageSerializer)
    assert isinstance(transport.deserializer, BlobMessageDeserializer)


@pytest.mark.asyncio
async def test_reclaim_lost_messages(loop, redis_client, redis_pool, dummy_api):
    """Test that messages which another consumer has timed out on can be reclaimed"""

    # Add a message
    await redis_client.xadd(
        "my.dummy.my_event:stream",
        fields={
            b"api_name": b"my.dummy",
            b"event_name": b"my_event",
            b"id": b"123",
            b"version": b"1",
            b":field": b'"value"',
        },
    )
    # Create the consumer group
    await redis_client.xgroup_create("my.dummy.my_event:stream", "test_group", latest_id="0")

    # Claim it in the name of another consumer
    await redis_client.xread_group(
        "test_group", "bad_consumer", ["my.dummy.my_event:stream"], latest_ids=[0]
    )
    # Sleep a moment to fake a short timeout
    await asyncio.sleep(0.02)

    event_transport = RedisEventTransport(
        redis_pool=redis_pool,
        consumer_group_prefix="test_group",
        consumer_name="good_consumer",
        acknowledgement_timeout=0.01,  # in ms, short for the sake of testing
        stream_use=StreamUse.PER_EVENT,
    )
    reclaimer = event_transport._reclaim_lost_messages(
        stream_names=["my.dummy.my_event:stream"],
        consumer_group="test_group",
        expected_events={"my_event"},
    )
    reclaimed_messages = [m async for m, id_ in reclaimer]
    assert len(reclaimed_messages) == 1
    assert reclaimed_messages[0].native_id
    assert type(reclaimed_messages[0].native_id) == str


@pytest.mark.asyncio
async def test_reclaim_lost_messages_ignores_non_timed_out_messages(
    loop, redis_client, redis_pool, dummy_api
):
    """Ensure messages which have not timed out are not reclaimed"""

    # Add a message
    await redis_client.xadd(
        "my.dummy.my_event:stream",
        fields={
            b"api_name": b"my.dummy",
            b"event_name": b"my_event",
            b"id": b"123",
            b":field": b'"value"',
        },
    )
    # Create the consumer group
    await redis_client.xgroup_create("my.dummy.my_event:stream", "test_group", latest_id="0")

    # Claim it in the name of another consumer
    await redis_client.xread_group(
        "test_group", "bad_consumer", ["my.dummy.my_event:stream"], latest_ids=[0]
    )
    # Sleep a moment to fake a short timeout
    await asyncio.sleep(0.02)

    event_transport = RedisEventTransport(
        redis_pool=redis_pool,
        consumer_group_prefix="test_group",
        consumer_name="good_consumer",
        # in ms, longer as we want to check that the messages is not reclaimed
        acknowledgement_timeout=0.9,
        stream_use=StreamUse.PER_EVENT,
    )
    reclaimer = event_transport._reclaim_lost_messages(
        stream_names=["my.dummy.my_event:stream"],
        consumer_group="test_group",
        expected_events={"my_event"},
    )
    reclaimed_messages = [m async for m in reclaimer]
    assert len(reclaimed_messages) == 0


@pytest.mark.asyncio
async def test_reclaim_lost_messages_consume(loop, redis_client, redis_pool, dummy_api):
    """Test that messages which another consumer has timed out on can be reclaimed

    Unlike the above test, we call consume() here, not _reclaim_lost_messages()
    """

    # Add a message
    await redis_client.xadd(
        "my.dummy.my_event:stream",
        fields={
            b"api_name": b"my.dummy",
            b"event_name": b"my_event",
            b"id": b"123",
            b"version": b"1",
            b":field": b'"value"',
        },
    )
    # Create the consumer group
    await redis_client.xgroup_create("my.dummy.my_event:stream", "test_group", latest_id="0")

    # Claim it in the name of another consumer
    await redis_client.xread_group(
        "test_group", "bad_consumer", ["my.dummy.my_event:stream"], latest_ids=[0]
    )
    # Sleep a moment to fake a short timeout
    await asyncio.sleep(0.02)

    event_transport = RedisEventTransport(
        redis_pool=redis_pool,
        consumer_group_prefix="",
        consumer_name="good_consumer",
        acknowledgement_timeout=0.01,  # in ms, short for the sake of testing
        stream_use=StreamUse.PER_EVENT,
    )
    consumer = event_transport.consume(
        listen_for=[("my.dummy", "my_event")], since="0", context={}, consumer_group="test_group"
    )

    messages = []

    async def consume():
        async for message in consumer:
            if isinstance(message, EventMessage):
                messages.append(message)

    task = asyncio.ensure_future(consume())
    await asyncio.sleep(0.1)
    assert len(messages) == 1
    await cancel(task)


@pytest.mark.asyncio
async def test_reclaim_pending_messages(loop, redis_client, redis_pool, dummy_api):
    """Test that unacked messages belonging to this consumer get reclaimed on startup
    """

    # Add a message
    await redis_client.xadd(
        "my.dummy.my_event:stream",
        fields={
            b"api_name": b"my.dummy",
            b"event_name": b"my_event",
            b"id": b"123",
            b"version": b"1",
            b":field": b'"value"',
        },
    )
    # Create the consumer group
    await redis_client.xgroup_create("my.dummy.my_event:stream", "test_group", latest_id="0")

    # Claim it in the name of ourselves
    await redis_client.xread_group(
        "test_group", "good_consumer", ["my.dummy.my_event:stream"], latest_ids=[0]
    )

    event_transport = RedisEventTransport(
        redis_pool=redis_pool,
        consumer_group_prefix="",
        consumer_name="good_consumer",
        stream_use=StreamUse.PER_EVENT,
    )
    consumer = event_transport.consume(
        listen_for=[("my.dummy", "my_event")], since="0", context={}, consumer_group="test_group"
    )

    messages = []

    async def consume():
        async for message in consumer:
            if isinstance(message, EventMessage):
                messages.append(message)

    task = asyncio.ensure_future(consume())
    await asyncio.sleep(0.1)
    assert len(messages) == 1
    assert messages[0].api_name == "my.dummy"
    assert messages[0].event_name == "my_event"
    assert messages[0].kwargs == {"field": "value"}
    assert messages[0].native_id
    assert type(messages[0].native_id) == str

    # Now check that redis believes the message has been consumed
    total_pending, *_ = await redis_client.xpending("my.dummy.my_event:stream", "test_group")
    assert total_pending == 0

    await cancel(task)


@pytest.mark.asyncio
async def test_consume_events_create_consumer_group_first(
    loop, redis_client, redis_event_transport, dummy_api
):
    """Create the consumer group before the stream exists

    This should create a noop message which gets ignored by the event transport
    """
    consumer = redis_event_transport.consume(
        listen_for=[("my.dummy", "my_event")], since="0", context={}, consumer_group="test_group"
    )
    messages = []

    async def consume():
        async for message in consumer:
            messages.append(message)

    task = asyncio.ensure_future(consume())
    await asyncio.sleep(0.1)
    assert len(messages) == 0
    await cancel(task)


@pytest.mark.asyncio
async def test_max_len_truncating(redis_event_transport: RedisEventTransport, redis_client, caplog):
    """Make sure the event stream gets truncated

    Note that truncation is approximate
    """
    caplog.set_level(logging.WARNING)
    redis_event_transport.max_stream_length = 100
    for x in range(0, 200):
        await redis_event_transport.send_event(
            EventMessage(api_name="my.api", event_name="my_event", kwargs={"field": "value"}),
            options={},
        )
    messages = await redis_client.xrange("my.api.my_event:stream")
    assert len(messages) >= 100
    assert len(messages) < 150


@pytest.mark.asyncio
async def test_max_len_set_to_none(
    redis_event_transport: RedisEventTransport, redis_client, caplog
):
    """Make sure the event stream does not get truncated when
    max_stream_length = None
    """
    caplog.set_level(logging.WARNING)
    redis_event_transport.max_stream_length = None
    for x in range(0, 200):
        await redis_event_transport.send_event(
            EventMessage(api_name="my.api", event_name="my_event", kwargs={"field": "value"}),
            options={},
        )
    messages = await redis_client.xrange("my.api.my_event:stream")
    assert len(messages) == 200


@pytest.mark.asyncio
async def test_consume_events_per_api_stream(
    loop, redis_event_transport: RedisEventTransport, redis_client, dummy_api
):
    redis_event_transport.stream_use = StreamUse.PER_API

    event_names = []

    async def co_consume(event_name):
        consumer = redis_event_transport.consume([("my.dummy", event_name)], {})
        async for message_ in consumer:
            event_names.append(message_.event_name)
            await consumer.__anext__()

    task1 = asyncio.ensure_future(co_consume("my_event1"))
    task2 = asyncio.ensure_future(co_consume("my_event2"))
    task3 = asyncio.ensure_future(co_consume("my_event3"))
    await asyncio.sleep(0.1)

    await redis_client.xadd(
        "my.dummy.*:stream",
        fields={
            b"api_name": b"my.dummy",
            b"event_name": b"my_event1",
            b"id": b"1",
            b"version": b"1",
            b":field": b'"value"',
        },
    )
    await redis_client.xadd(
        "my.dummy.*:stream",
        fields={
            b"api_name": b"my.dummy",
            b"event_name": b"my_event2",
            b"id": b"2",
            b"version": b"1",
            b":field": b'"value"',
        },
    )
    await redis_client.xadd(
        "my.dummy.*:stream",
        fields={
            b"api_name": b"my.dummy",
            b"event_name": b"my_event3",
            b"id": b"3",
            b"version": b"1",
            b":field": b'"value"',
        },
    )
    await asyncio.sleep(0.1)

    assert len(event_names) == 3
    assert set(event_names) == {"my_event1", "my_event2", "my_event3"}

    await cancel(task1, task2, task3)


@pytest.mark.asyncio
async def test_reconnect_upon_send_event(
    redis_event_transport: RedisEventTransport, redis_client, get_total_redis_connections
):
    assert await get_total_redis_connections() == 2
    await redis_client.execute(b"CLIENT", b"KILL", b"TYPE", b"NORMAL")
    assert await get_total_redis_connections() == 1

    await redis_event_transport.send_event(
        EventMessage(api_name="my.api", event_name="my_event", id="123", kwargs={"field": "value"}),
        options={},
    )
    messages = await redis_client.xrange("my.api.my_event:stream")
    assert len(messages) == 1
    assert await get_total_redis_connections() == 2


@pytest.mark.asyncio
async def test_reconnect_while_listening(
    loop, redis_event_transport: RedisEventTransport, redis_client, dummy_api
):
    redis_event_transport.consumption_restart_delay = 0.0001

    async def co_enqeue():
        while True:
            await asyncio.sleep(0.1)
            logging.info("test_reconnect_while_listening: Sending message")
            await redis_client.xadd(
                "my.dummy.my_event:stream",
                fields={
                    b"api_name": b"my.dummy",
                    b"event_name": b"my_event",
                    b"id": b"123",
                    b"version": b"1",
                    b":field": b'"value"',
                },
            )

    total_messages = 0

    async def co_consume():
        nonlocal total_messages

        consumer = redis_event_transport.consume([("my.dummy", "my_event")], {})
        async for message_ in consumer:
            total_messages += 1
            await consumer.__anext__()

    enque_task = asyncio.ensure_future(co_enqeue())
    consume_task = asyncio.ensure_future(co_consume())

    await asyncio.sleep(0.2)
    assert total_messages > 0
    await redis_client.execute(b"CLIENT", b"KILL", b"TYPE", b"NORMAL")
    total_messages = 0
    await asyncio.sleep(0.2)
    assert total_messages > 0

    await cancel(enque_task, consume_task)
