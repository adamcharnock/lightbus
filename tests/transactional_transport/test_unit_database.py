import pytest

from lightbus import EventMessage
from lightbus.exceptions import UnsupportedOptionValue
from lightbus.transports.transactional import DbApiConnection

pytestmark = pytest.mark.unit


@pytest.mark.run_loop
async def test_migrate(dbapi_database: DbApiConnection, get_processed_events, get_outbox):
    await dbapi_database.migrate()
    await dbapi_database.commit_transaction()

    # The following would fail if the tables didn't exist
    assert len(await get_processed_events()) == 0
    assert len(await get_outbox()) == 0


@pytest.mark.run_loop
async def test_transaction_commit(dbapi_database: DbApiConnection, get_processed_events):
    await dbapi_database.migrate()
    await dbapi_database.store_processed_event(
        EventMessage(api_name="api", event_name="event", id="123")
    )
    await dbapi_database.commit_transaction()
    assert len(await get_processed_events()) == 1


@pytest.mark.run_loop
async def test_transaction_rollback(dbapi_database: DbApiConnection, get_processed_events):
    await dbapi_database.migrate()
    # We're about to rollback, so make sure we commit our migration first
    await dbapi_database.commit_transaction()
    await dbapi_database.start_transaction()

    await dbapi_database.store_processed_event(
        EventMessage(api_name="api", event_name="event", id="123")
    )
    await dbapi_database.rollback_transaction()
    assert len(await get_processed_events()) == 0


@pytest.mark.run_loop
async def test_transaction_rollback_continue(dbapi_database: DbApiConnection, get_processed_events):
    # Check we can still use the connection following a rollback
    await dbapi_database.migrate()
    # We're about to rollback, so make sure we commit our migration first
    await dbapi_database.commit_transaction()
    await dbapi_database.start_transaction()

    await dbapi_database.store_processed_event(
        EventMessage(api_name="api", event_name="event", id="123")
    )
    await dbapi_database.rollback_transaction()  # Rollback
    await dbapi_database.store_processed_event(
        EventMessage(api_name="api", event_name="event", id="123")
    )

    assert len(await get_processed_events()) == 1


@pytest.mark.run_loop
async def test_is_event_duplicate_true(dbapi_database: DbApiConnection):
    await dbapi_database.migrate()
    message = EventMessage(api_name="api", event_name="event", id="123")
    await dbapi_database.store_processed_event(message)
    assert await dbapi_database.is_event_duplicate(message) == True


@pytest.mark.run_loop
async def test_is_event_duplicate_false(dbapi_database: DbApiConnection):
    await dbapi_database.migrate()
    message = EventMessage(api_name="api", event_name="event", id="123")
    assert await dbapi_database.is_event_duplicate(message) == False


@pytest.mark.run_loop
async def test_send_event_ok(dbapi_database: DbApiConnection, get_outbox):
    await dbapi_database.migrate()

    message = EventMessage(api_name="api", event_name="event", kwargs={"field": "abc"}, id="123")
    options = {"key": "value"}
    await dbapi_database.send_event(message, options)
    await dbapi_database.commit_transaction()

    assert len(await get_outbox()) == 1
    retrieved_message, options = await dbapi_database.consume_pending_events(
        message_id="123"
    ).__anext__()
    assert retrieved_message.id == "123"
    assert retrieved_message.get_kwargs() == {"field": "abc"}
    assert retrieved_message.get_metadata() == {
        "api_name": "api",
        "event_name": "event",
        "id": "123",
        "version": 1,
    }
    assert options == {"key": "value"}


@pytest.mark.run_loop
async def test_send_event_bad_option_value(dbapi_database: DbApiConnection):
    await dbapi_database.migrate()
    message = EventMessage(api_name="api", event_name="event", kwargs={"field": "abc"}, id="123")
    options = {"key": range(1, 100)}  # not json-serializable
    with pytest.raises(UnsupportedOptionValue):
        await dbapi_database.send_event(message, options)


@pytest.mark.run_loop
async def test_remove_pending_event(dbapi_database: DbApiConnection, get_outbox):
    await dbapi_database.migrate()
    message = EventMessage(api_name="api", event_name="event", kwargs={"field": "abc"}, id="123")
    await dbapi_database.send_event(message, options={})
    await dbapi_database.commit_transaction()

    assert len(await get_outbox()) == 1
    await dbapi_database.remove_pending_event(message_id="123")
    assert len(await get_outbox()) == 0
