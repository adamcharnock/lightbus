import asyncio

import pytest

from lightbus import (
    TransactionalEventTransport,
    RedisEventTransport,
    DebugEventTransport,
    EventMessage,
)
from lightbus.config import Config
from lightbus.transports.transactional.transport import ConnectionAlreadySet, DatabaseNotSet
from lightbus.utilities.async import block
from tests.transactional_transport.conftest import verification_connection

pytestmark = pytest.mark.unit


# Utilities


async def active_transactions():
    async with verification_connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT *  "
                "FROM pg_stat_activity "
                "WHERE (state = 'idle in transaction') "
                "AND xact_start IS NOT NULL;"
            )
            res = await cursor.fetchall()
            return len(res)


@pytest.fixture()
def transaction_transport(aiopg_connection, aiopg_cursor, loop):
    transport = TransactionalEventTransport(DebugEventTransport())
    block(transport.set_connection(aiopg_connection, aiopg_cursor), timeout=1)
    block(transport.database.migrate(), timeout=1)
    return transport


async def consumer_to_messages(consumer):
    messages = []
    async for message in consumer:
        messages.append(message)
        dummy_value = await consumer.__anext__()
        assert not isinstance(dummy_value, EventMessage)
    return messages


@pytest.fixture()
def transaction_transport_with_consumer(aiopg_connection, aiopg_cursor):
    # Create a transaction transport which will produce some pre-defined events
    async def make_it(event_messages):

        async def dummy_fetcher(*args, **kwargs):
            for event_message in event_messages:
                yield event_message
                yield True

        transport = TransactionalEventTransport(DebugEventTransport())
        # start_transaction=False, as we start a transaction below (using BEGIN)
        await transport.set_connection(aiopg_connection, aiopg_cursor)
        await aiopg_cursor.execute("BEGIN -- transaction_transport_with_consumer")
        await transport.database.migrate()
        # Commit the migrations
        await aiopg_cursor.execute("COMMIT -- transaction_transport_with_consumer")
        transport.child_transport.fetch = dummy_fetcher
        return transport

    return make_it


# Tests


def test_from_config():
    transport = TransactionalEventTransport.from_config(
        config=Config.load_dict({}),
        child_transport={"redis": {"url": "redis://foo/1"}},
        database_class="lightbus.transports.transactional.DbApiConnection",
    )
    assert isinstance(transport.child_transport, RedisEventTransport)
    assert transport.child_transport.connection_parameters["address"] == "redis://foo/1"


@pytest.mark.run_loop
async def test_set_connection_ok(aiopg_connection, aiopg_cursor):
    transport = TransactionalEventTransport(DebugEventTransport())
    await transport.set_connection(aiopg_connection, aiopg_cursor)
    assert transport.connection == aiopg_connection
    assert transport.cursor == aiopg_cursor
    assert await active_transactions() == 0


@pytest.mark.run_loop
async def test_set_connection_already_set(aiopg_connection, aiopg_cursor):
    transport = TransactionalEventTransport(DebugEventTransport())
    await transport.set_connection(aiopg_connection, aiopg_cursor)
    with pytest.raises(ConnectionAlreadySet):
        await transport.set_connection(aiopg_connection, aiopg_cursor)


@pytest.mark.run_loop
async def test_commit_and_finish_no_database():
    transport = TransactionalEventTransport(DebugEventTransport())
    with pytest.raises(DatabaseNotSet):
        await transport.commit_and_finish()


@pytest.mark.run_loop
async def test_commit_and_finish_ok(transaction_transport):
    await transaction_transport.database.start_transaction()
    await transaction_transport.commit_and_finish()
    assert await active_transactions() == 0

    assert transaction_transport.connection is None
    assert transaction_transport.cursor is None
    assert transaction_transport.database is None


@pytest.mark.run_loop
async def test_rollback_and_finish_ok(transaction_transport):
    await transaction_transport.database.start_transaction()
    await transaction_transport.rollback_and_finish()
    assert await active_transactions() == 0

    assert transaction_transport.connection is None
    assert transaction_transport.cursor is None
    assert transaction_transport.database is None


@pytest.mark.run_loop
async def test_send_event(mocker, transaction_transport):
    f = asyncio.Future()
    f.set_result(None)
    m = mocker.patch.object(transaction_transport.database, "send_event", return_value=f)
    message = EventMessage(api_name="api", event_name="event", id="123")

    await transaction_transport.send_event(event_message=message, options={"a": 1})
    assert m.called
    args, kwargs = m.call_args
    assert args == (message, {"a": 1})


@pytest.mark.run_loop
async def test_fetch_ok(transaction_transport_with_consumer, aiopg_cursor, loop):
    message1 = EventMessage(api_name="api", event_name="event", id="1")
    message2 = EventMessage(api_name="api", event_name="event", id="2")
    message3 = EventMessage(api_name="api", event_name="event", id="3")

    transport = await transaction_transport_with_consumer(
        event_messages=[message1, message2, message3]
    )
    consumer = transport.consume(listen_for="api.event", context={})
    produced_events = await consumer_to_messages(consumer)
    assert produced_events == [message1, message2, message3]

    await aiopg_cursor.execute("SELECT COUNT(*) FROM lightbus_processed_events")
    total_processed_events = (await aiopg_cursor.fetchone())[0]
    assert total_processed_events == 3


@pytest.mark.run_loop
async def test_fetch_duplicate(transaction_transport_with_consumer, aiopg_cursor, loop):
    # Same IDs = duplicate messages
    message1 = EventMessage(api_name="api", event_name="event", id="1")
    message2 = EventMessage(api_name="api", event_name="event", id="1")

    transport = await transaction_transport_with_consumer(event_messages=[message1, message2])
    consumer = transport.consume(listen_for="api.event", context={})
    produced_events = await consumer_to_messages(consumer)
    assert produced_events == [message1]  # The second message should be ignored

    await aiopg_cursor.execute("SELECT COUNT(*) FROM lightbus_processed_events")
    total_processed_events = (await aiopg_cursor.fetchone())[0]
    assert total_processed_events == 1


@pytest.mark.run_loop
async def test_publish_pending(transaction_transport, mocker):
    f = asyncio.Future()
    f.set_result(None)
    m = mocker.patch.object(transaction_transport.child_transport, "send_event", return_value=f)

    message1 = EventMessage(api_name="api", event_name="event", id="1")
    message2 = EventMessage(api_name="api", event_name="event", id="2")
    message3 = EventMessage(api_name="api", event_name="event", id="3")

    await transaction_transport.database.send_event(message1, options={"a": "1"})
    await transaction_transport.database.send_event(message2, options={"a": "2"})
    await transaction_transport.database.send_event(message3, options={"a": "3"})

    await transaction_transport.publish_pending()

    assert m.call_count == 3
    messages = [c[0][0] for c in m.call_args_list]
    message_ids = [message.id for message in messages]
    assert message_ids == ["1", "2", "3"]
