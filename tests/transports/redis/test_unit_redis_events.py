import asyncio
import logging
from datetime import datetime
from itertools import chain

import aioredis
import pytest
from aioredis import Redis

from lightbus.config import Config
from lightbus.message import EventMessage
from lightbus.serializers import (
    ByFieldMessageSerializer,
    BlobMessageSerializer,
    BlobMessageDeserializer,
)
from lightbus.transports.redis.event import StreamUse
from lightbus.transports.redis.utilities import RedisEventMessage
from lightbus import RedisEventTransport
from lightbus.utilities.async_tools import cancel
from tests.conftest import StandaloneRedisServer

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
async def test_consume_events_simple(
    redis_event_transport: RedisEventTransport, redis_client, error_queue
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
        async for message_ in redis_event_transport.consume(
            [("my.dummy", "my_event")], "test_listener", error_queue=error_queue
        ):
            return message_

    enqueue_result, messages = await asyncio.gather(co_enqeue(), co_consume())
    message = messages[0]
    assert message.api_name == "my.dummy"
    assert message.event_name == "my_event"
    assert message.kwargs == {"field": "value"}
    assert message.native_id
    assert type(message.native_id) == str


@pytest.mark.asyncio
async def test_consume_events_noop(
    redis_event_transport: RedisEventTransport, redis_client, error_queue
):
    """Consume a noop event"""
    messages = []

    async def co_consume():
        nonlocal messages
        async for message_ in redis_event_transport.consume(
            [("my.dummy", "my_event")], "test_listener", since="0", error_queue=error_queue
        ):
            messages.append(message_)

    consume_task = asyncio.ensure_future(co_consume())

    await redis_client.xadd("my.dummy.my_event:stream", fields={b"": b""})

    await asyncio.sleep(0.05)
    await cancel(consume_task)

    assert not messages
    consumers = await redis_client.xinfo_consumers(
        "my.dummy.my_event:stream", "test_service-test_listener"
    )

    assert len(consumers) == 1
    assert consumers[0][b"pending"] == 0


@pytest.mark.asyncio
async def test_consume_events_noop_reclaim(
    redis_event_transport: RedisEventTransport, redis_client, error_queue
):
    """Reclaim a noop event"""
    messages = []

    async def co_consume():
        nonlocal messages
        async for message_ in redis_event_transport.consume(
            [("my.dummy", "my_event")], "test_listener", since="0", error_queue=error_queue
        ):
            messages.append(message_)

    await redis_client.xadd("my.dummy.my_event:stream", fields={b"": b""})
    assert await redis_client.xgroup_create(
        "my.dummy.my_event:stream", "test_service-test_listener", latest_id="0"
    )
    assert await redis_client.xread_group(
        "test_service-test_listener",
        "test_consumer",
        ["my.dummy.my_event:stream"],
        latest_ids=[">"],
        timeout=None,
    )

    groups = await redis_client.xinfo_groups("my.dummy.my_event:stream")
    consumers = await redis_client.xinfo_consumers(
        "my.dummy.my_event:stream", "test_service-test_listener"
    )
    # We now have one pending event
    assert consumers[0][b"pending"] == 1

    # Now consume it again (using the same consumer name)
    consume_task = asyncio.ensure_future(co_consume())
    await asyncio.sleep(0.05)
    await cancel(consume_task)

    assert not messages
    consumers = await redis_client.xinfo_consumers(
        "my.dummy.my_event:stream", "test_service-test_listener"
    )

    assert len(consumers) == 1
    assert consumers[0][b"pending"] == 0


@pytest.mark.asyncio
async def test_consume_events_multiple_consumers(redis_pool, redis_client, error_queue):
    messages = []

    async def co_consume(group_number):
        event_transport = RedisEventTransport(
            redis_pool=redis_pool,
            service_name=f"test_service{group_number}",
            consumer_name=f"test_consumer",
            stream_use=StreamUse.PER_EVENT,
        )

        async for messages_ in event_transport.consume(
            [("my.dummy", "my_event")], "test_listener", error_queue=error_queue
        ):
            messages.append(messages_)
            await event_transport.acknowledge(*messages_)

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

    assert len(messages) == 2


@pytest.mark.asyncio
async def test_consume_events_multiple_consumers_one_group(redis_pool, redis_client, error_queue):
    events = []

    async def co_consume(consumer_number):
        event_transport = RedisEventTransport(
            redis_pool=redis_pool,
            service_name="test_service",
            consumer_name=f"test_consumer{consumer_number}",
            stream_use=StreamUse.PER_EVENT,
        )
        consumer = event_transport.consume(
            listen_for=[("my.dummy", "my_event")],
            listener_name="test_listener",
            error_queue=error_queue,
        )
        async for messages in consumer:
            events.append(messages)
            await event_transport.acknowledge(*messages)

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

    assert len(events) == 1


@pytest.mark.asyncio
async def test_consume_events_since_id(
    redis_event_transport: RedisEventTransport, redis_client, error_queue
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
        [("my.dummy", "my_event")],
        "cg",
        since="1515000001500-0",
        forever=False,
        error_queue=error_queue,
    )

    events = []

    async def co():
        async for messages in consumer:
            events.extend(messages)
            await redis_event_transport.acknowledge(*messages)

    task = asyncio.ensure_future(co())
    await asyncio.sleep(0.1)
    await cancel(task)

    messages_ids = [m.native_id for m in events if isinstance(m, EventMessage)]
    assert len(messages_ids) == 2
    assert len(events) == 2
    assert events[0].kwargs["field"] == "2"
    assert events[1].kwargs["field"] == "3"


@pytest.mark.asyncio
async def test_consume_events_since_datetime(
    redis_event_transport: RedisEventTransport, redis_client, error_queue
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
        [("my.dummy", "my_event")],
        listener_name="test",
        since=since_datetime,
        forever=False,
        error_queue=error_queue,
    )

    events = []

    async def co():
        async for messages in consumer:
            events.extend(messages)
            await redis_event_transport.acknowledge(*messages)

    task = asyncio.ensure_future(co())
    await asyncio.sleep(0.1)
    await cancel(task)

    assert len(events) == 2
    assert events[0].kwargs["field"] == "2"
    assert events[1].kwargs["field"] == "3"


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

    assert transport._redis_pool.connection.maxsize == 123
    assert isinstance(transport.serializer, BlobMessageSerializer)
    assert isinstance(transport.deserializer, BlobMessageDeserializer)


@pytest.mark.asyncio
async def test_reclaim_lost_messages_one(redis_client: Redis, redis_pool):
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
    await redis_client.xgroup_create(
        stream="my.dummy.my_event:stream", group_name="test_service", latest_id="0"
    )

    # Claim it in the name of another consumer
    result = await redis_client.xread_group(
        group_name="test_service",
        consumer_name="bad_consumer",
        streams=["my.dummy.my_event:stream"],
        latest_ids=[">"],
    )
    assert result, "Didn't actually manage to claim any message"

    # Sleep a moment to fake a short timeout
    await asyncio.sleep(0.1)

    event_transport = RedisEventTransport(
        redis_pool=redis_pool,
        service_name="test_service",
        consumer_name="good_consumer",
        acknowledgement_timeout=0.01,  # in ms, short for the sake of testing
        stream_use=StreamUse.PER_EVENT,
    )
    reclaimer = event_transport._reclaim_lost_messages(
        stream_names=["my.dummy.my_event:stream"],
        consumer_group="test_service",
        expected_events={"my_event"},
    )

    reclaimed_messages = []
    async for m in reclaimer:
        reclaimed_messages.extend(m)
        for m in reclaimed_messages:
            await event_transport.acknowledge(m)

    assert len(reclaimed_messages) == 1
    assert reclaimed_messages[0].native_id
    assert type(reclaimed_messages[0].native_id) == str

    result = await redis_client.xinfo_consumers("my.dummy.my_event:stream", "test_service")
    consumer_info = {r[b"name"]: r for r in result}

    assert consumer_info[b"bad_consumer"][b"pending"] == 0
    assert consumer_info[b"good_consumer"][b"pending"] == 0


@pytest.mark.asyncio
async def test_reclaim_lost_messages_different_event(redis_client: Redis, redis_pool):
    """Test that messages which another consumer has timed out on can be reclaimed

    However, in this case we have a single stream for an entire API. The stream
    has a lost message for an event we are not listening for. In this case the
    event shouldn't be claimed
    """

    # Add a message
    await redis_client.xadd(
        "my.dummy.*:stream",
        fields={
            b"api_name": b"my.dummy",
            b"event_name": b"my_event",
            b"id": b"123",
            b"version": b"1",
            b":field": b'"value"',
        },
    )
    # Create the consumer group
    await redis_client.xgroup_create(
        stream="my.dummy.*:stream", group_name="test_service", latest_id="0"
    )

    # Claim it in the name of another consumer
    result = await redis_client.xread_group(
        group_name="test_service",
        consumer_name="bad_consumer",
        streams=["my.dummy.*:stream"],
        latest_ids=[">"],
    )
    assert result, "Didn't actually manage to claim any message"

    # Sleep a moment to fake a short timeout
    await asyncio.sleep(0.1)

    event_transport = RedisEventTransport(
        redis_pool=redis_pool,
        service_name="test_service",
        consumer_name="good_consumer",
        acknowledgement_timeout=0.01,  # in ms, short for the sake of testing
        stream_use=StreamUse.PER_API,
    )
    reclaimer = event_transport._reclaim_lost_messages(
        stream_names=["my.dummy.*:stream"],
        consumer_group="test_service",
        expected_events={"another_event"},  # NOTE: We this is NOT the event we created above
    )

    reclaimed_messages = []
    async for m in reclaimer:
        reclaimed_messages.extend(m)
        for m in reclaimed_messages:
            await event_transport.acknowledge(m)

    assert len(reclaimed_messages) == 0

    result = await redis_client.xinfo_consumers("my.dummy.*:stream", "test_service")
    consumer_info = {r[b"name"]: r for r in result}

    assert consumer_info[b"bad_consumer"][b"pending"] == 0
    assert consumer_info[b"good_consumer"][b"pending"] == 0


@pytest.mark.asyncio
async def test_reclaim_lost_messages_many(redis_client, redis_pool):
    """Test that messages keep getting reclaimed until there are none left"""

    # Add 20 messages
    for x in range(0, 20):
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
    await redis_client.xgroup_create(
        stream="my.dummy.my_event:stream", group_name="test_service", latest_id="0"
    )

    # Claim them all in the name of another consumer
    result = await redis_client.xread_group(
        group_name="test_service",
        consumer_name="bad_consumer",
        streams=["my.dummy.my_event:stream"],
        latest_ids=[">"],
        count=20,
    )
    assert result, "Didn't actually manage to claim any message"

    # Sleep a moment to fake a short timeout
    await asyncio.sleep(0.1)

    event_transport = RedisEventTransport(
        redis_pool=redis_pool,
        service_name="test_service",
        consumer_name="good_consumer",
        acknowledgement_timeout=0.01,  # in ms, short for the sake of testing
        stream_use=StreamUse.PER_EVENT,
        reclaim_batch_size=5,  # Less than 20 (see message creation above), so multiple fetches required
    )

    reclaimer = event_transport._reclaim_lost_messages(
        stream_names=["my.dummy.my_event:stream"],
        consumer_group="test_service",
        expected_events={"my_event"},
    )

    # We should get 20 unique IDs back (i.e. no duplicates, nothing missed)
    messages = chain(*[m async for m in reclaimer])
    message_ids = {m.native_id for m in messages}
    assert len(message_ids) == 20


@pytest.mark.asyncio
async def test_reclaim_lost_messages_ignores_non_timed_out_messages(redis_client, redis_pool):
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
    await redis_client.xgroup_create(
        stream="my.dummy.my_event:stream", group_name="test_service", latest_id="0"
    )

    # Claim it in the name of another consumer
    await redis_client.xread_group(
        group_name="test_service",
        consumer_name="bad_consumer",
        streams=["my.dummy.my_event:stream"],
        latest_ids=[">"],
    )
    # Sleep a moment to fake a short timeout
    await asyncio.sleep(0.1)

    event_transport = RedisEventTransport(
        redis_pool=redis_pool,
        service_name="test_service",
        consumer_name="good_consumer",
        # in ms, longer as we want to check that the messages is not reclaimed
        acknowledgement_timeout=0.9,
        stream_use=StreamUse.PER_EVENT,
    )
    reclaimer = event_transport._reclaim_lost_messages(
        stream_names=["my.dummy.my_event:stream"],
        consumer_group="test_service",
        expected_events={"my_event"},
    )
    reclaimed_messages = [m async for m in reclaimer]
    assert len(reclaimed_messages) == 0


@pytest.mark.asyncio
async def test_reclaim_lost_messages_consume(redis_client, redis_pool, error_queue):
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
    await redis_client.xgroup_create(
        stream="my.dummy.my_event:stream", group_name="test_service-test_listener", latest_id="0"
    )

    # Claim it in the name of another consumer
    await redis_client.xread_group(
        group_name="test_service-test_listener",
        consumer_name="bad_consumer",
        streams=["my.dummy.my_event:stream"],
        latest_ids=[">"],
    )
    # Sleep a moment to fake a short timeout
    await asyncio.sleep(0.1)

    event_transport = RedisEventTransport(
        redis_pool=redis_pool,
        service_name="test_service",
        consumer_name="good_consumer",
        acknowledgement_timeout=0.01,  # in ms, short for the sake of testing
        stream_use=StreamUse.PER_EVENT,
    )
    consumer = event_transport.consume(
        listen_for=[("my.dummy", "my_event")],
        since="0",
        listener_name="test_listener",
        error_queue=error_queue,
    )

    messages = []

    async def consume():
        async for messages_ in consumer:
            messages.extend(messages_)
            # Ack the messages, otherwise the message will get picked up in the
            # claiming (good) and then, because it hasn't been acked, get picked
            # up by the consume too (bad).
            await event_transport.acknowledge(*messages_)

    task = asyncio.ensure_future(consume())
    await asyncio.sleep(0.1)
    await cancel(task)

    assert len(messages) == 1


@pytest.mark.asyncio
async def test_reclaim_pending_messages(redis_client, redis_pool, error_queue):
    """Test that unacked messages belonging to this consumer get reclaimed on startup"""

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
    await redis_client.xgroup_create(
        stream="my.dummy.my_event:stream", group_name="test_service-test_listener", latest_id="0"
    )

    # Claim it in the name of ourselves
    await redis_client.xread_group(
        group_name="test_service-test_listener",
        consumer_name="good_consumer",
        streams=["my.dummy.my_event:stream"],
        latest_ids=[">"],
    )

    event_transport = RedisEventTransport(
        redis_pool=redis_pool,
        service_name="test_service",
        consumer_name="good_consumer",
        stream_use=StreamUse.PER_EVENT,
    )
    consumer = event_transport.consume(
        listen_for=[("my.dummy", "my_event")],
        since="0",
        listener_name="test_listener",
        error_queue=error_queue,
    )

    messages = []

    async def consume():
        async for messages_ in consumer:
            messages.extend(messages_)
            await event_transport.acknowledge(*messages_)

    task = asyncio.ensure_future(consume())
    await asyncio.sleep(0.1)
    await cancel(task)

    assert len(messages) == 1
    assert messages[0].api_name == "my.dummy"
    assert messages[0].event_name == "my_event"
    assert messages[0].kwargs == {"field": "value"}
    assert messages[0].native_id
    assert type(messages[0].native_id) == str

    # Now check that redis believes the message has been consumed
    total_pending, *_ = await redis_client.xpending(
        stream="my.dummy.my_event:stream", group_name="test_service-test_listener"
    )
    assert total_pending == 0


@pytest.mark.asyncio
async def test_consume_events_create_consumer_group_first(redis_event_transport, error_queue):
    """Create the consumer group before the stream exists

    This should create a noop message which gets ignored by the event transport
    """
    consumer = redis_event_transport.consume(
        listen_for=[("my.dummy", "my_event")],
        since="0",
        listener_name="test_listener",
        error_queue=error_queue,
    )
    messages = []

    async def consume():
        async for messages in consumer:
            messages.extend(messages)

    task = asyncio.ensure_future(consume())
    await asyncio.sleep(0.1)
    await cancel(task)
    assert len(messages) == 0


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
    redis_event_transport: RedisEventTransport, redis_client, error_queue
):
    redis_event_transport.stream_use = StreamUse.PER_API

    events = []

    async def co_consume(event_name, consumer_group):
        consumer = redis_event_transport.consume(
            [("my.dummy", event_name)], consumer_group, error_queue=error_queue
        )
        async for messages in consumer:
            events.extend(messages)

    task1 = asyncio.ensure_future(co_consume("my_event1", "cg1"))
    task2 = asyncio.ensure_future(co_consume("my_event2", "cg2"))
    task3 = asyncio.ensure_future(co_consume("my_event3", "cg3"))
    await asyncio.sleep(0.2)

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
    await asyncio.sleep(0.2)
    await cancel(task1, task2, task3)

    assert set([e.event_name for e in events]) == {"my_event1", "my_event2", "my_event3"}


@pytest.mark.asyncio
async def test_reconnect_upon_send_event(
    redis_event_transport: RedisEventTransport, redis_client, get_total_redis_connections
):
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
async def test_reconnect_while_listening_connection_dropped(
    redis_event_transport: RedisEventTransport, redis_client, error_queue
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

        consumer = redis_event_transport.consume(
            [("my.dummy", "my_event")], "test_listener", error_queue=error_queue
        )
        async for messages_ in consumer:
            total_messages += len(messages_)
            await redis_event_transport.acknowledge(*messages_)
            logging.info(f"Received {len(messages_)} messages. Total now at {total_messages}")

    enqueue_task = asyncio.ensure_future(co_enqeue())
    consume_task = asyncio.ensure_future(co_consume())

    await asyncio.sleep(0.2)
    assert total_messages > 0
    await redis_client.execute(b"CLIENT", b"KILL", b"TYPE", b"NORMAL")
    total_messages = 0
    await asyncio.sleep(0.2)

    await cancel(enqueue_task, consume_task)
    assert total_messages > 0


@pytest.mark.asyncio
async def test_reconnect_while_listening_dead_server(
    standalone_redis_server: StandaloneRedisServer, create_redis_pool, dummy_api, error_queue
):
    redis_url = f"redis://127.0.0.1:{standalone_redis_server.port}/0"
    standalone_redis_server.start()

    redis_event_transport = RedisEventTransport(
        redis_pool=await create_redis_pool(address=redis_url),
        consumption_restart_delay=0.0001,
        service_name="test",
        consumer_name="test",
        stream_use=StreamUse.PER_EVENT,
    )

    async def co_enqeue():
        redis_client = await aioredis.create_redis(address=redis_url)
        try:
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
        finally:
            redis_client.close()

    total_messages = 0

    async def co_consume():
        nonlocal total_messages

        consumer = redis_event_transport.consume(
            [("my.dummy", "my_event")], "test_listener", error_queue=error_queue
        )
        async for messages_ in consumer:
            total_messages += len(messages_)
            await redis_event_transport.acknowledge(*messages_)
            logging.info(f"Received {len(messages_)} messages. Total now at {total_messages}")

    # Starting enqeuing and consuming events
    enqueue_task = asyncio.ensure_future(co_enqeue())
    consume_task = asyncio.ensure_future(co_consume())

    await asyncio.sleep(0.2)
    assert total_messages > 0

    # Stop enqeuing and stop the server
    await cancel(enqueue_task)
    standalone_redis_server.stop()

    # We don't get any more messages
    total_messages = 0
    await asyncio.sleep(0.2)
    assert total_messages == 0

    try:
        # Now start the server again, and start emitting messages
        standalone_redis_server.start()
        enqueue_task = asyncio.ensure_future(co_enqeue())
        total_messages = 0
        await asyncio.sleep(0.2)
        # ... the consumer has auto-reconnected and received some messages
        assert total_messages > 0
    finally:
        await cancel(enqueue_task, consume_task)


@pytest.mark.asyncio
async def test_acknowledge(redis_event_transport: RedisEventTransport, redis_client, dummy_api):
    message_id = await redis_client.xadd("test_api.test_event:stream", fields={"a": 1})
    await redis_client.xgroup_create("test_api.test_event:stream", "test_group", latest_id="0")

    messages = await redis_client.xread_group(
        "test_group", "test_consumer", ["test_api.test_event:stream"], latest_ids=[">"]
    )
    assert len(messages) == 1

    total_pending, *_ = await redis_client.xpending("test_api.test_event:stream", "test_group")
    assert total_pending == 1

    await redis_event_transport.acknowledge(
        RedisEventMessage(
            api_name="test_api",
            event_name="test_event",
            consumer_group="test_group",
            stream="test_api.test_event:stream",
            native_id=message_id,
        )
    )

    total_pending, *_ = await redis_client.xpending("test_api.test_event:stream", "test_group")
    assert total_pending == 0


@pytest.mark.asyncio
async def test_cleanup_consumer_deleted_only(
    redis_event_transport: RedisEventTransport, redis_client
):
    """Test that a single consumer within a group is deleted"""
    # Add a couple of messages for our consumers to fetch
    await redis_client.xadd("test_stream", {"noop": ""})
    await redis_client.xadd("test_stream", {"noop": ""})
    # Create the group
    await redis_client.xgroup_create("test_stream", "test_group", latest_id="0")

    # Create group test_consumer_1 by performing a read.
    await redis_client.xread_group(
        group_name="test_group",
        consumer_name="test_consumer_1",
        streams=["test_stream"],
        latest_ids=[">"],
        timeout=None,
        count=1,
    )

    # Wait a moment to force test_consumer_1 to look old
    await asyncio.sleep(0.100)

    # Create group test_consumer_2 by performing a read.
    await redis_client.xread_group(
        group_name="test_group",
        consumer_name="test_consumer_2",
        streams=["test_stream"],
        latest_ids=[">"],
        timeout=None,
        count=1,
    )

    # Set a very low ttl
    redis_event_transport.consumer_ttl = 0.050

    # Do it
    await redis_event_transport._cleanup(stream_names=["test_stream"])

    groups = await redis_client.xinfo_groups("test_stream")
    consumers = await redis_client.xinfo_consumers("test_stream", "test_group")
    assert len(groups) == 1, groups
    assert len(consumers) == 1, consumers
    assert consumers[0][b"name"] == b"test_consumer_2"


@pytest.mark.asyncio
async def test_cleanup_group_deleted(redis_event_transport: RedisEventTransport, redis_client):
    """Test that a whole group gets deleted when all the consumers get cleaned up"""
    # Add a couple of messages for our consumers to fetch
    await redis_client.xadd("test_stream", {"noop": ""})
    await redis_client.xadd("test_stream", {"noop": ""})
    # Create the group
    await redis_client.xgroup_create("test_stream", "test_group", latest_id="0")

    # Create group test_consumer_1 by performing a read.
    await redis_client.xread_group(
        group_name="test_group",
        consumer_name="test_consumer_1",
        streams=["test_stream"],
        latest_ids=[">"],
        timeout=None,
        count=1,
    )

    # Create group test_consumer_2 by performing a read.
    await redis_client.xread_group(
        group_name="test_group",
        consumer_name="test_consumer_2",
        streams=["test_stream"],
        latest_ids=[">"],
        timeout=None,
        count=1,
    )

    # Wait a moment to force test_consumer_1 & test_consumer_2 to look old
    await asyncio.sleep(0.100)

    # Set a very low ttl
    redis_event_transport.consumer_ttl = 0.050

    # Do it
    await redis_event_transport._cleanup(stream_names=["test_stream"])

    groups = await redis_client.xinfo_groups("test_stream")
    assert len(groups) == 0, groups


@pytest.mark.asyncio
async def test_history_get_all_single_batch(
    redis_event_transport: RedisEventTransport, redis_client
):
    message = EventMessage(native_id="", api_name="my_api", event_name="my_event")
    data = ByFieldMessageSerializer()(message)
    await redis_client.xadd("my_api.my_event:stream", data, message_id=b"1-0")
    await redis_client.xadd("my_api.my_event:stream", data, message_id=b"2-0")
    await redis_client.xadd("my_api.my_event:stream", data, message_id=b"3-0")
    await redis_client.xadd("my_api.my_event:stream", data, message_id=b"4-0")
    await redis_client.xadd("my_api.my_event:stream", data, message_id=b"5-0")

    messages = redis_event_transport.history("my_api", "my_event", batch_size=100)
    message_ids = {m.native_id async for m in messages}
    assert len(message_ids) == 5


@pytest.mark.asyncio
async def test_history_get_all_multiple_batches(
    redis_event_transport: RedisEventTransport, redis_client
):
    message = EventMessage(native_id="", api_name="my_api", event_name="my_event")
    data = ByFieldMessageSerializer()(message)
    await redis_client.xadd("my_api.my_event:stream", data, message_id=b"1-0")
    await redis_client.xadd("my_api.my_event:stream", data, message_id=b"2-0")
    await redis_client.xadd("my_api.my_event:stream", data, message_id=b"3-0")
    await redis_client.xadd("my_api.my_event:stream", data, message_id=b"4-0")
    await redis_client.xadd("my_api.my_event:stream", data, message_id=b"5-0")

    messages = redis_event_transport.history("my_api", "my_event", batch_size=2)
    message_ids = {m.native_id async for m in messages}
    assert len(message_ids) == 5


@pytest.mark.asyncio
async def test_history_get_subset_single_batch(
    redis_event_transport: RedisEventTransport, redis_client
):
    message = EventMessage(native_id="", api_name="my_api", event_name="my_event")
    data = ByFieldMessageSerializer()(message)
    await redis_client.xadd("my_api.my_event:stream", data, message_id=b"1-0")
    await redis_client.xadd("my_api.my_event:stream", data, message_id=b"2-0")
    await redis_client.xadd("my_api.my_event:stream", data, message_id=b"3-0")
    await redis_client.xadd("my_api.my_event:stream", data, message_id=b"4-0")
    await redis_client.xadd("my_api.my_event:stream", data, message_id=b"5-0")

    messages = redis_event_transport.history(
        "my_api",
        "my_event",
        batch_size=100,
        start=datetime.fromtimestamp(0.002),
        stop=datetime.fromtimestamp(0.004),
    )
    message_ids = {m.native_id async for m in messages}
    assert message_ids == {"2-0", "3-0", "4-0"}


@pytest.mark.asyncio
async def test_history_get_subset_multiple_batches(
    redis_event_transport: RedisEventTransport, redis_client
):
    message = EventMessage(native_id="", api_name="my_api", event_name="my_event")
    data = ByFieldMessageSerializer()(message)
    await redis_client.xadd("my_api.my_event:stream", data, message_id=b"1-0")
    await redis_client.xadd("my_api.my_event:stream", data, message_id=b"2-0")
    await redis_client.xadd("my_api.my_event:stream", data, message_id=b"3-0")
    await redis_client.xadd("my_api.my_event:stream", data, message_id=b"4-0")
    await redis_client.xadd("my_api.my_event:stream", data, message_id=b"5-0")

    messages = redis_event_transport.history(
        "my_api",
        "my_event",
        batch_size=2,
        start=datetime.fromtimestamp(0.002),
        stop=datetime.fromtimestamp(0.004),
    )
    message_ids = {m.native_id async for m in messages}
    assert message_ids == {"2-0", "3-0", "4-0"}


@pytest.mark.asyncio
async def test_lag_no_stream(redis_event_transport: RedisEventTransport, redis_client, error_queue):
    """No stream exists"""
    lag = await redis_event_transport.lag(
        api_name="my_api", event_name="my_event", listener_name="my_listener",
    )
    assert lag == 0


@pytest.mark.asyncio
async def test_lag_no_consumer(
    redis_event_transport: RedisEventTransport, redis_client, error_queue
):
    """No consumer exists"""
    await redis_client.xadd("my_api.my_event:stream", {"a": 1}, message_id=b"1-0")
    lag = await redis_event_transport.lag(
        api_name="my_api", event_name="my_event", listener_name="my_listener",
    )
    assert lag == 0


@pytest.mark.asyncio
async def test_lag_no_matching_group(
    redis_event_transport: RedisEventTransport, redis_client, error_queue
):
    """A group exists but it is the wrong group"""
    await redis_client.xadd("my_api.my_event:stream", {"a": 1}, message_id=b"1-0")
    await redis_client.xgroup_create("my_api.my_event:stream", "another_listener", latest_id="0")
    lag = await redis_event_transport.lag(
        api_name="my_api", event_name="my_event", listener_name="my_listener",
    )
    assert lag == 0


@pytest.mark.asyncio
async def test_lag_no_lag(redis_event_transport: RedisEventTransport, redis_client, error_queue):
    """The stream & group exist, and there is no lag"""
    await redis_client.xadd("my_api.my_event:stream", {"a": 1}, message_id=b"1-0")
    await redis_client.xgroup_create(
        "my_api.my_event:stream", "test_service-my_listener", latest_id="1-0"
    )
    lag = await redis_event_transport.lag(
        api_name="my_api", event_name="my_event", listener_name="my_listener",
    )
    assert lag == 0


@pytest.mark.asyncio
async def test_lag_has_lag_no_pending(
    redis_event_transport: RedisEventTransport, redis_client, error_queue
):
    """The stream & group exist, there is lag, and there are no pending messages"""
    await redis_client.xadd("my_api.my_event:stream", {"a": 1}, message_id=b"1-0")
    await redis_client.xadd("my_api.my_event:stream", {"a": 1}, message_id=b"2-0")
    await redis_client.xadd("my_api.my_event:stream", {"a": 1}, message_id=b"3-0")
    await redis_client.xgroup_create(
        "my_api.my_event:stream", "test_service-my_listener", latest_id="1-0"
    )
    lag = await redis_event_transport.lag(
        api_name="my_api", event_name="my_event", listener_name="my_listener",
    )
    assert lag == 2


@pytest.mark.asyncio
async def test_lag_no_lag_latest_message_missing(
    redis_event_transport: RedisEventTransport, redis_client, error_queue
):
    """The stream & group exist, there is no lag, and the message pointed to by latest_id does not exist"""
    await redis_client.xadd("my_api.my_event:stream", {"a": 1}, message_id=b"1-0")
    await redis_client.xgroup_create(
        "my_api.my_event:stream", "test_service-my_listener", latest_id="2-0"
    )
    lag = await redis_event_transport.lag(
        api_name="my_api", event_name="my_event", listener_name="my_listener",
    )
    assert lag == 0


@pytest.mark.asyncio
async def test_lag_has_lag_has_pending(
    redis_event_transport: RedisEventTransport, redis_client, error_queue
):
    """The stream & group exist, there is lag, and there are pending messages"""
    await redis_client.xadd("my_api.my_event:stream", {"a": 1}, message_id=b"1-0")
    await redis_client.xadd("my_api.my_event:stream", {"a": 2}, message_id=b"2-0")
    await redis_client.xadd("my_api.my_event:stream", {"a": 3}, message_id=b"3-0")
    await redis_client.xgroup_create(
        "my_api.my_event:stream", "test_service-my_listener", latest_id="1-0"
    )
    # Do the read. The message will therefore be pending, but the
    # consumer groups latest_id will point to 2-0
    message = await redis_client.xread_group(
        group_name="test_service-my_listener",
        consumer_name="test_consumer",
        streams=["my_api.my_event:stream"],
        latest_ids=[">"],
        count=1,
    )
    lag = await redis_event_transport.lag(
        api_name="my_api", event_name="my_event", listener_name="my_listener",
    )
    assert lag == 2


@pytest.mark.asyncio
async def test_lag_no_lag_has_pending(
    redis_event_transport: RedisEventTransport, redis_client, error_queue
):
    """The stream & group exist, there is no lag, and there are pending messages"""
    await redis_client.xadd("my_api.my_event:stream", {"a": 1}, message_id=b"1-0")
    await redis_client.xadd("my_api.my_event:stream", {"a": 2}, message_id=b"2-0")
    await redis_client.xgroup_create(
        "my_api.my_event:stream", "test_service-my_listener", latest_id="1-0"
    )
    # Do the read. The message will therefore be pending, but the
    # consumer groups latest_id will point to 2-0
    message = await redis_client.xread_group(
        group_name="test_service-my_listener",
        consumer_name="test_consumer",
        streams=["my_api.my_event:stream"],
        latest_ids=[">"],
        count=1,
    )
    lag = await redis_event_transport.lag(
        api_name="my_api", event_name="my_event", listener_name="my_listener",
    )
    assert lag == 1


@pytest.mark.asyncio
async def test_lag_max_count_set(
    redis_event_transport: RedisEventTransport, redis_client, error_queue
):
    """The stream & group exist, there is lag, and there are no pending messages"""
    for _ in range(0, 20):
        await redis_client.xadd("my_api.my_event:stream", {"a": 1})

    await redis_client.xgroup_create(
        "my_api.my_event:stream", "test_service-my_listener", latest_id="1-0"
    )
    lag = await redis_event_transport.lag(
        api_name="my_api", event_name="my_event", listener_name="my_listener", max_count=15
    )
    assert lag == 15


@pytest.mark.asyncio
async def test_lag_max_count_none(
    redis_event_transport: RedisEventTransport, redis_client, error_queue
):
    """Check that passing max_count=None calculates the full stream lag"""
    for _ in range(0, 20):
        await redis_client.xadd("my_api.my_event:stream", {"a": 1})

    await redis_client.xgroup_create(
        "my_api.my_event:stream", "test_service-my_listener", latest_id="1-0"
    )
    lag = await redis_event_transport.lag(
        api_name="my_api", event_name="my_event", listener_name="my_listener", max_count=None
    )
    assert lag == 20


@pytest.mark.asyncio
async def test_lag_max_count_none_latest_id_doesnt_exist(
    redis_event_transport: RedisEventTransport, redis_client, error_queue
):
    """Check that passing max_count=None calculates the full stream lag
    even when the latest_id doesn't exist"""
    for _ in range(0, 20):
        await redis_client.xadd("my_api.my_event:stream", {"a": 1})

    await redis_client.xgroup_create(
        "my_api.my_event:stream", "test_service-my_listener", latest_id="0-0"
    )
    lag = await redis_event_transport.lag(
        api_name="my_api", event_name="my_event", listener_name="my_listener", max_count=None
    )
    assert lag == 20
