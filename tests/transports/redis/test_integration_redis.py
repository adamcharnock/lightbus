import asyncio
import logging
from typing import Type
from unittest.mock import MagicMock

import jsonschema
import pytest

import lightbus
import lightbus.path
from lightbus.client.commands import SendResultCommand
from lightbus.config import Config
from lightbus.exceptions import LightbusTimeout, LightbusWorkerError
from lightbus.transports.redis.event import StreamUse
from lightbus.utilities.async_tools import cancel
from lightbus.utilities.features import Feature
from lightbus.utilities.testing import BusQueueMockerContext
from tests.conftest import Worker

pytestmark = pytest.mark.integration

stream_use_test_data = [StreamUse.PER_EVENT, StreamUse.PER_API]

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.timeout(5)
@pytest.mark.also_run_in_child_thread
async def test_rpc_simple(bus: lightbus.path.BusPath, dummy_api):
    """Full rpc call integration test"""
    bus.client.register_api(dummy_api)

    await bus.client.consume_rpcs(apis=[dummy_api])

    result = await bus.my.dummy.my_proc.call_async(field="Hello! 😎")
    assert result == "value: Hello! 😎"


@pytest.mark.asyncio
async def test_rpc_timeout(bus: lightbus.path.BusPath, dummy_api):
    """Full rpc call integration test"""
    bus.client.register_api(dummy_api)

    await bus.client.consume_rpcs(apis=[dummy_api])

    with pytest.raises(LightbusTimeout):
        await bus.my.dummy.sudden_death.call_async(n=0)


@pytest.mark.asyncio
async def test_rpc_error(bus: lightbus.path.BusPath, dummy_api, worker: Worker, caplog):
    """Test what happens when the remote procedure throws an error"""
    bus.client.register_api(dummy_api)
    caplog.set_level(logging.ERROR)

    bus.client.register_api(dummy_api)
    async with worker(bus):
        with pytest.raises(LightbusWorkerError):
            await bus.my.dummy.general_error.call_async()

        # Event loop not stopped, because RPCs should continue to be served
        # even in the case of an error
        assert not bus.client.stop_loop.called
        assert len(caplog.records) == 1
        assert "Oh no, there was some kind of error" in caplog.records[0].message


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stream_use", stream_use_test_data, ids=["stream_per_event", "stream_per_api"]
)
async def test_event_simple(
    bus: lightbus.path.BusPath, dummy_api, stream_use, redis_client, worker: Worker
):
    """Full event integration test"""
    bus.client.set_features([Feature.EVENTS])
    bus.client.register_api(dummy_api)

    event_transport_pool = bus.client.transport_registry.get_event_transport("default")
    async with event_transport_pool as t1, event_transport_pool as t2:
        # The pool will need two transports for this, so get two from the pool and set
        # their stream_use config option
        t1.stream_use = stream_use
        t2.stream_use = stream_use

    received_messages = []

    async def listener(event_message, **kwargs):
        nonlocal received_messages
        received_messages.append(event_message)

    bus.my.dummy.my_event.listen(listener, listener_name="test")

    async with worker(bus):
        await asyncio.sleep(0.1)
        await bus.my.dummy.my_event.fire_async(field="Hello! 😎")
        await asyncio.sleep(0.1)

    assert len(received_messages) == 1
    assert received_messages[0].kwargs == {"field": "Hello! 😎"}
    assert received_messages[0].api_name == "my.dummy"
    assert received_messages[0].event_name == "my_event"
    assert received_messages[0].native_id

    # Check the event was acknowledged
    stream_name = (
        "my.dummy.my_event:stream" if stream_use == StreamUse.PER_EVENT else "my.dummy.*:stream"
    )
    info = await redis_client.xinfo_groups(stream=stream_name)
    assert (
        len(info) == 1
    ), "There should only be one consumer group, that for out event listener above"
    assert info[0][b"pending"] == 0


@pytest.mark.asyncio
async def test_ids(
    bus: lightbus.path.BusPath, dummy_api, queue_mocker: Type[BusQueueMockerContext]
):
    """Ensure the id comes back correctly"""
    bus.client.register_api(dummy_api)

    await bus.client.consume_rpcs(apis=[dummy_api])

    with queue_mocker(bus.client) as q:
        await bus.my.dummy.my_proc.call_async(field="foo")

    send_result_command = q.rpc_result.to_transport.commands.get(SendResultCommand)

    assert send_result_command.rpc_message.id
    assert send_result_command.message.id
    assert send_result_command.message.rpc_message_id == send_result_command.rpc_message.id


class ApiA(lightbus.Api):
    event_a = lightbus.Event()

    class Meta:
        name = "api_a"

    def rpc_a(self):
        return "A"


class ApiB(lightbus.Api):
    event_b = lightbus.Event()

    class Meta:
        name = "api_b"

    def rpc_b(self):
        return "b"


@pytest.mark.asyncio
async def test_multiple_rpc_transports(loop, redis_server_url, redis_server_b_url, consume_rpcs):
    """Configure a bus with two redis transports and ensure they write to the correct redis servers"""
    redis_url_a = redis_server_url
    redis_url_b = redis_server_b_url

    logging.warning(f"Server A url: {redis_url_a}")
    logging.warning(f"Server B url: {redis_url_b}")

    config = Config.load_dict(
        {
            "bus": {"schema": {"transport": {"redis": {"url": redis_url_a}}}},
            "apis": {
                "default": {
                    "rpc_transport": {"redis": {"url": redis_url_a}},
                    "result_transport": {"redis": {"url": redis_url_a}},
                },
                "api_b": {
                    "rpc_transport": {"redis": {"url": redis_url_b}},
                    "result_transport": {"redis": {"url": redis_url_b}},
                },
            },
        }
    )

    bus = lightbus.create(config=config)
    bus.client.register_api(ApiA())
    bus.client.register_api(ApiB())

    task = asyncio.ensure_future(consume_rpcs(bus))
    await asyncio.sleep(0.1)

    await bus.api_a.rpc_a.call_async()
    await bus.api_b.rpc_b.call_async()
    await asyncio.sleep(0.1)

    await cancel(task)
    await bus.client.close_async()


@pytest.mark.asyncio
async def test_multiple_event_transports(
    loop, redis_server_url, redis_server_b_url, create_redis_client
):
    """Configure a bus with two redis transports and ensure they write to the correct redis servers"""
    redis_url_a = redis_server_url
    redis_url_b = redis_server_b_url

    logging.warning(f"Server A URL: {redis_url_a}")
    logging.warning(f"Server B URL: {redis_url_b}")

    config = Config.load_dict(
        {
            "bus": {"schema": {"transport": {"redis": {"url": redis_url_a}}}},
            "apis": {
                "default": {
                    "event_transport": {
                        "redis": {"url": redis_url_a, "stream_use": StreamUse.PER_EVENT.value}
                    }
                },
                "api_b": {
                    "event_transport": {
                        "redis": {"url": redis_url_b, "stream_use": StreamUse.PER_EVENT.value}
                    }
                },
            },
        }
    )

    bus = lightbus.create(config=config)
    bus.client.register_api(ApiA())
    bus.client.register_api(ApiB())
    await asyncio.sleep(0.1)

    await bus.api_a.event_a.fire_async()
    await bus.api_b.event_b.fire_async()

    redis_a = await create_redis_client(address=redis_server_url)
    redis_b = await create_redis_client(address=redis_server_b_url)

    assert await redis_a.xrange("api_a.event_a:stream")
    assert await redis_a.xrange("api_b.event_b:stream") == []

    assert await redis_b.xrange("api_a.event_a:stream") == []
    assert await redis_b.xrange("api_b.event_b:stream")

    await bus.client.close_async()


@pytest.mark.asyncio
async def test_validation_rpc(loop, bus: lightbus.path.BusPath, dummy_api, mocker):
    """Check validation happens when performing an RPC"""
    bus.client.register_api(dummy_api)
    config = Config.load_dict({"apis": {"default": {"validate": True, "strict_validation": True}}})
    bus.client.config = config
    mocker.patch("jsonschema.validate", autospec=True)

    async def co_consume_rpcs():
        return await bus.client.consume_rpcs(apis=[dummy_api])

    await bus.client.schema.add_api(dummy_api)
    await bus.client.schema.save_to_bus()
    await bus.client.schema.load_from_bus()

    consume_task = asyncio.ensure_future(co_consume_rpcs(), loop=loop)

    await asyncio.sleep(0.1)
    result = await bus.my.dummy.my_proc.call_async(field="Hello")

    await cancel(consume_task)

    assert result == "value: Hello"

    # Validate gets called
    jsonschema.validate.assert_called_with(
        "value: Hello",
        {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "RPC my.dummy.my_proc() response",
            "type": "string",
        },
    )


@pytest.mark.asyncio
async def test_validation_event(
    loop, bus: lightbus.path.BusPath, dummy_api, mocker, worker: Worker
):
    """Check validation happens when firing an event"""
    bus.client.register_api(dummy_api)
    config = Config.load_dict({"apis": {"default": {"validate": True, "strict_validation": True}}})
    bus.client.config = config
    mocker.patch("jsonschema.validate", autospec=True)

    async def co_listener(*a, **kw):
        pass

    await bus.client.schema.add_api(dummy_api)
    await bus.client.schema.save_to_bus()
    await bus.client.schema.load_from_bus()

    bus.client.listen_for_event("my.dummy", "my_event", co_listener, listener_name="test")

    async with worker(bus):
        await asyncio.sleep(0.1)
        await bus.my.dummy.my_event.fire_async(field="Hello")
        await asyncio.sleep(0.001)

    # Validate gets called
    jsonschema.validate.assert_called_with(
        {"field": "Hello"},
        {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "additionalProperties": False,
            "properties": {"field": {"type": "string"}},
            "required": ["field"],
            "title": "Event my.dummy.my_event parameters",
        },
    )


@pytest.mark.asyncio
async def test_listen_to_multiple_events_across_multiple_transports(
    loop, redis_server_url, redis_server_b_url, worker: Worker
):
    redis_url_a = redis_server_url
    redis_url_b = redis_server_b_url

    logging.warning(f"Server A URL: {redis_url_a}")
    logging.warning(f"Server B URL: {redis_url_b}")

    config = Config.load_dict(
        {
            "bus": {"schema": {"transport": {"redis": {"url": redis_url_a}}}},
            "apis": {
                "default": {"event_transport": {"redis": {"url": redis_url_a}}},
                "api_b": {"event_transport": {"redis": {"url": redis_url_b}}},
            },
        }
    )

    bus = lightbus.create(config=config)
    bus.client.register_api(ApiA())
    bus.client.register_api(ApiB())
    await asyncio.sleep(0.1)

    calls = 0

    def listener(*args, **kwargs):
        nonlocal calls
        calls += 1

    bus.client.listen_for_events(
        events=[("api_a", "event_a"), ("api_b", "event_b")], listener=listener, listener_name="test"
    )

    async with worker(bus):
        await asyncio.sleep(0.1)
        await bus.api_a.event_a.fire_async()
        await bus.api_b.event_b.fire_async()
        await asyncio.sleep(0.1)

    assert calls == 2


@pytest.mark.asyncio
async def test_event_exception_in_listener_realtime(
    bus: lightbus.path.BusPath, new_bus, worker: Worker, dummy_api, redis_client
):
    """Start a listener (which errors) and then add events to the stream.
    The listener will load them one-by-one."""
    bus.client.register_api(dummy_api)
    received_messages = []

    async def listener(event_message, **kwargs):
        nonlocal received_messages
        received_messages.append(event_message)
        raise Exception()

    worker_bus = new_bus()
    bus.client.proxied_client.stop_loop = MagicMock()
    worker_bus.my.dummy.my_event.listen(
        listener, listener_name="test_listener", bus_options={"since": "0"}
    )

    async with worker(worker_bus, raise_errors=False):
        await bus.client.lazy_load_now()
        await asyncio.sleep(0.1)
        await bus.my.dummy.my_event.fire_async(field="Hello! 😎")
        await bus.my.dummy.my_event.fire_async(field="Hello! 😎")
        await bus.my.dummy.my_event.fire_async(field="Hello! 😎")
        await bus.my.dummy.my_event.fire_async(field="Hello! 😎")
        await bus.my.dummy.my_event.fire_async(field="Hello! 😎")
        await asyncio.sleep(0.1)

    # Ensure the server stopped the loop (we've mocked this otherwise this
    # test would be stopped too)
    assert worker_bus.client.stop_loop.called

    # Died when processing first message, so we only saw one message
    assert len(received_messages) == 1

    # Now check we have not acked any messages

    messages = await redis_client.xrange("my.dummy.my_event:stream")
    # Messages 0 is the noop message used to create the stream
    message_ids = [id_ for id_, *_ in messages]

    pending_messages = await redis_client.xpending(
        "my.dummy.my_event:stream", "test_service-test_listener", "-", "+", 10, "test_consumer"
    )

    pending_message_ids = [id_ for id_, *_ in pending_messages]

    # The erroneous message is still pending
    assert len(pending_message_ids) == 1


@pytest.mark.asyncio
async def test_event_exception_in_listener_batch_fetch(
    bus: lightbus.path.BusPath, dummy_api, redis_client, worker: Worker
):
    """Add a number of events to a stream then startup a listener which errors.
    The listener will fetch them all at once."""
    bus.client.register_api(dummy_api)
    bus.client.features = [Feature.EVENTS]
    received_messages = []

    async def listener(event_message, **kwargs):
        nonlocal received_messages
        received_messages.append(event_message)
        raise Exception()

    await bus.client.lazy_load_now()

    await bus.my.dummy.my_event.fire_async(field="Hello! 😎")
    await bus.my.dummy.my_event.fire_async(field="Hello! 😎")
    await bus.my.dummy.my_event.fire_async(field="Hello! 😎")

    bus.my.dummy.my_event.listen(
        listener, listener_name="test_listener", bus_options={"since": "0"}
    )

    async with worker(bus):
        await asyncio.sleep(0.2)

    # Died when processing first message, so we only saw one message
    assert len(received_messages) == 1

    # Now check we have not acked any of them

    messages = await redis_client.xrange("my.dummy.my_event:stream")
    # No message0 here because the stream already exists (because we've just added events to it)
    message1_id, message2_id, message3_id = [id_ for id_, *_ in messages]

    pending_messages = await redis_client.xpending(
        "my.dummy.my_event:stream", "test_service-test_listener", "-", "+", 10, "test_consumer"
    )

    assert len(pending_messages) == 3
