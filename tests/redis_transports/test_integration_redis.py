import asyncio
import logging

import jsonschema
import pytest
from aioredis.util import decode

import lightbus
import lightbus.path
from lightbus.config.structure import OnError
from lightbus.path import BusPath
from lightbus.api import registry
from lightbus.config import Config
from lightbus.exceptions import LightbusTimeout, LightbusServerError
from lightbus.plugins import manually_set_plugins
from lightbus.transports.redis import StreamUse
from lightbus.utilities.async_tools import cancel

pytestmark = pytest.mark.integration

stream_use_test_data = [StreamUse.PER_EVENT, StreamUse.PER_API]


@pytest.mark.asyncio
async def test_bus_fixture(bus: lightbus.path.BusPath):
    """Just sanity check the fixture"""
    rpc_transport = bus.client.transport_registry.get_rpc_transport("default")
    result_transport = bus.client.transport_registry.get_result_transport("default")
    event_transport = bus.client.transport_registry.get_event_transport("default")

    connection_manager_rpc = await rpc_transport.connection_manager()
    connection_manager_result = await result_transport.connection_manager()
    connection_manager_event = await event_transport.connection_manager()

    with await connection_manager_rpc as redis_rpc, await connection_manager_result as redis_result, await connection_manager_event as redis_event:

        # Each transport should have its own connection
        assert redis_rpc is not redis_result
        assert redis_result is not redis_event
        assert redis_rpc is not redis_event

        # Check they are all looking at the same redis instance
        await redis_rpc.set("X", 1)
        assert await redis_result.get("X")
        assert await redis_rpc.get("X")

        info = await redis_rpc.info()
        # transports: rpc, result, event, schema
        assert int(info["clients"]["connected_clients"]) == 4


@pytest.mark.asyncio
async def test_rpc(bus: lightbus.path.BusPath, dummy_api):
    """Full rpc call integration test"""

    async def co_call_rpc():
        await asyncio.sleep(0.1)
        return await bus.my.dummy.my_proc.call_async(field="Hello! 😎")

    async def co_consume_rpcs():
        return await bus.client.consume_rpcs(apis=[dummy_api])

    (call_task,), (consume_task,) = await asyncio.wait(
        [co_call_rpc(), co_consume_rpcs()], return_when=asyncio.FIRST_COMPLETED
    )
    consume_task.cancel()
    assert call_task.result() == "value: Hello! 😎"


@pytest.mark.asyncio
async def test_rpc_timeout(bus: lightbus.path.BusPath, dummy_api):
    """Full rpc call integration test"""

    async def co_call_rpc():
        await asyncio.sleep(0.1)
        return await bus.my.dummy.sudden_death.call_async(n=0)

    async def co_consume_rpcs():
        return await bus.client.consume_rpcs(apis=[dummy_api])

    (call_task,), (consume_task,) = await asyncio.wait(
        [co_call_rpc(), co_consume_rpcs()], return_when=asyncio.FIRST_COMPLETED
    )
    consume_task.cancel()

    with pytest.raises(LightbusTimeout):
        call_task.result()


@pytest.mark.asyncio
async def test_rpc_error(bus: lightbus.path.BusPath, dummy_api):
    """Test what happens when the remote procedure throws an error"""

    async def co_call_rpc():
        await asyncio.sleep(0.1)
        return await bus.my.dummy.general_error.call_async()

    async def co_consume_rpcs():
        return await bus.client.consume_rpcs(apis=[dummy_api])

    (call_task,), (consume_task,) = await asyncio.wait(
        [co_call_rpc(), co_consume_rpcs()], return_when=asyncio.FIRST_COMPLETED
    )

    consume_task.cancel()
    call_task.cancel()

    with pytest.raises(LightbusServerError):
        await call_task.result()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stream_use", stream_use_test_data, ids=["stream_per_event", "stream_per_api"]
)
async def test_event_simple(bus: lightbus.path.BusPath, dummy_api, stream_use):
    """Full event integration test"""
    bus.client.transport_registry.get_event_transport("default").stream_use = stream_use
    manually_set_plugins({})
    received_messages = []

    async def listener(event_message, **kwargs):
        nonlocal received_messages
        received_messages.append(event_message)

    await bus.my.dummy.my_event.listen_async(listener)
    await asyncio.sleep(0.01)
    await bus.my.dummy.my_event.fire_async(field="Hello! 😎")
    await asyncio.sleep(0.01)

    # await asyncio.gather(co_fire_event(), co_listen_for_events())
    assert len(received_messages) == 1
    assert received_messages[0].kwargs == {"field": "Hello! 😎"}
    assert received_messages[0].api_name == "my.dummy"
    assert received_messages[0].event_name == "my_event"
    assert received_messages[0].native_id


@pytest.mark.asyncio
async def test_ids(bus: lightbus.path.BusPath, dummy_api, mocker):
    """Ensure the id comes back correctly"""

    async def co_call_rpc():
        await asyncio.sleep(0.1)
        return await bus.my.dummy.my_proc.call_async(field="foo")

    async def co_consume_rpcs():
        return await bus.client.consume_rpcs(apis=[dummy_api])

    mocker.spy(bus.client, "send_result")

    (call_task,), (consume_task,) = await asyncio.wait(
        [co_call_rpc(), co_consume_rpcs()], return_when=asyncio.FIRST_COMPLETED
    )
    _, kw = bus.client.send_result.call_args
    rpc_message = kw["rpc_message"]
    result_message = kw["result_message"]
    consume_task.cancel()

    assert rpc_message.id
    assert result_message.rpc_message_id
    assert rpc_message.id == result_message.rpc_message_id


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
    registry.add(ApiA())
    registry.add(ApiB())

    manually_set_plugins(plugins={})

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

    bus = BusPath(name="", parent=None, client=lightbus.BusClient(config=config))
    task = asyncio.ensure_future(consume_rpcs(bus))
    await asyncio.sleep(0.1)

    await bus.api_a.rpc_a.call_async()
    await bus.api_b.rpc_b.call_async()
    await asyncio.sleep(0.1)

    await cancel(task)
    await bus.client.close_async()


@pytest.mark.asyncio
async def test_multiple_event_transports(loop, redis_server_url, redis_server_b_url):
    """Configure a bus with two redis transports and ensure they write to the correct redis servers"""
    registry.add(ApiA())
    registry.add(ApiB())

    manually_set_plugins(plugins={})

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

    bus = BusPath(name="", parent=None, client=lightbus.BusClient(config=config))
    await asyncio.sleep(0.1)

    await bus.api_a.event_a.fire_async()
    await bus.api_b.event_b.fire_async()

    connection_manager_a = bus.client.transport_registry.get_event_transport(
        "api_a"
    ).connection_manager
    connection_manager_b = bus.client.transport_registry.get_event_transport(
        "api_b"
    ).connection_manager

    with await connection_manager_a() as redis:
        assert await redis.xrange("api_a.event_a:stream")
        assert await redis.xrange("api_b.event_b:stream") == []

    with await connection_manager_b() as redis:
        assert await redis.xrange("api_a.event_a:stream") == []
        assert await redis.xrange("api_b.event_b:stream")

    await bus.client.close_async()


@pytest.mark.asyncio
async def test_validation_rpc(loop, bus: lightbus.path.BusPath, dummy_api, mocker):
    """Check validation happens when performing an RPC"""
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
            "$schema": "http://json-schema.org/draft-04/schema#",
            "title": "RPC my.dummy.my_proc() response",
            "type": "string",
        },
    )


@pytest.mark.asyncio
async def test_validation_event(loop, bus: lightbus.path.BusPath, dummy_api, mocker):
    """Check validation happens when firing an event"""
    config = Config.load_dict(
        {"apis": {"default": {"validate": True, "strict_validation": True, "on_error": "ignore"}}}
    )
    bus.client.config = config
    mocker.patch("jsonschema.validate", autospec=True)

    async def co_listener(*a, **kw):
        pass

    await bus.client.schema.add_api(dummy_api)
    await bus.client.schema.save_to_bus()
    await bus.client.schema.load_from_bus()

    listener_task = await bus.client.listen_for_event("my.dummy", "my_event", co_listener)

    await asyncio.sleep(0.1)
    await bus.my.dummy.my_event.fire_async(field="Hello")
    await asyncio.sleep(0.001)

    await cancel(listener_task)

    # Validate gets called
    jsonschema.validate.assert_called_with(
        {"field": "Hello"},
        {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "type": "object",
            "additionalProperties": False,
            "properties": {"field": {"type": "string"}},
            "required": ["field"],
            "title": "Event my.dummy.my_event parameters",
        },
    )


@pytest.mark.asyncio
async def test_listen_to_multiple_events_across_multiple_transports(
    loop, redis_server_url, redis_server_b_url
):
    registry.add(ApiA())
    registry.add(ApiB())

    manually_set_plugins(plugins={})

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

    bus = BusPath(name="", parent=None, client=lightbus.BusClient(config=config))
    await asyncio.sleep(0.1)

    calls = 0

    def listener(*args, **kwargs):
        nonlocal calls
        calls += 1

    await bus.client.listen_for_events(
        events=[("api_a", "event_a"), ("api_b", "event_b")], listener=listener
    )

    await asyncio.sleep(0.1)
    await bus.api_a.event_a.fire_async()
    await bus.api_b.event_b.fire_async()
    await asyncio.sleep(0.1)

    await bus.client.close_async()

    assert calls == 2


@pytest.mark.asyncio
async def test_event_exception_in_listener_realtime(
    bus: lightbus.path.BusPath, dummy_api, redis_client
):
    """Start a listener (which errors) and then add events to the stream.
    The listener will load them one-by-one."""
    manually_set_plugins({})
    received_messages = []

    # Don't shutdown on error
    bus.client.config.api("default").on_error = OnError.STOP_LISTENER

    async def listener(event_message, **kwargs):
        nonlocal received_messages
        received_messages.append(event_message)
        raise Exception()

    await bus.my.dummy.my_event.listen_async(listener, bus_options={"since": "0"})
    await asyncio.sleep(0.1)

    await bus.my.dummy.my_event.fire_async(field="Hello! 😎")
    await bus.my.dummy.my_event.fire_async(field="Hello! 😎")
    await bus.my.dummy.my_event.fire_async(field="Hello! 😎")
    await bus.my.dummy.my_event.fire_async(field="Hello! 😎")
    await bus.my.dummy.my_event.fire_async(field="Hello! 😎")
    await asyncio.sleep(0.01)

    # Died when processing first message, so we only saw one message
    assert len(received_messages) == 1

    # Now check we have not acked any messages

    messages = await redis_client.xrange("my.dummy.my_event:stream")
    # Messages 0 is the noop message used to create the stream
    message_ids = [id_ for id_, *_ in messages]

    pending_messages = await redis_client.xpending(
        "my.dummy.my_event:stream", "test_service-default", "-", "+", 10, "test_consumer"
    )
    pending_message_ids = [id_ for id_, *_ in pending_messages]
    # The first 4 messages are still pending. Why 4 messages? Because:
    #  - 1. The noop message used to create the stream (because we listened before we fired)
    #  - 2. The first message which caused the error

    assert len(pending_message_ids) == 2
    assert pending_message_ids == message_ids[:2]


@pytest.mark.asyncio
async def test_event_exception_in_listener_batch_fetch(
    bus: lightbus.path.BusPath, dummy_api, redis_client
):
    """Add a number of events to a stream the startup a listener which errors.
    The listener will fetch them all at once."""
    manually_set_plugins({})
    received_messages = []

    # Don't shutdown on error
    bus.client.config.api("default").on_error = OnError.STOP_LISTENER

    async def listener(event_message, **kwargs):
        nonlocal received_messages
        received_messages.append(event_message)
        raise Exception()

    await bus.my.dummy.my_event.fire_async(field="Hello! 😎")
    await bus.my.dummy.my_event.fire_async(field="Hello! 😎")
    await bus.my.dummy.my_event.fire_async(field="Hello! 😎")

    await bus.my.dummy.my_event.listen_async(listener, bus_options={"since": "0"})
    await asyncio.sleep(0.1)

    # Died when processing first message, so we only saw one message
    assert len(received_messages) == 1

    # Now check we have not acked any of them

    messages = await redis_client.xrange("my.dummy.my_event:stream")
    # No message0 here because the stream already exists (because we've just added events to it)
    message1_id, message2_id, message3_id = [id_ for id_, *_ in messages]

    pending_messages = await redis_client.xpending(
        "my.dummy.my_event:stream", "test_service-default", "-", "+", 10, "test_consumer"
    )

    assert len(pending_messages) == 3
