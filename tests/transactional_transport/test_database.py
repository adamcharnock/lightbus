import json

import asyncpg
import pytest

from lightbus import EventMessage
from lightbus.transports.transactional import AsyncPostgresConnection
from tests.transactional_transport.conftest import verification_connection


async def total_processed_events():
    connection = await verification_connection()
    return await connection.fetchval('SELECT COUNT(*) FROM lightbus_processed_events')


async def outbox_size():
    connection = await verification_connection()
    return await connection.fetchval('SELECT COUNT(*) FROM lightbus_event_outbox')


async def get_processed_events():
    connection = await verification_connection()
    return await connection.fetch('SELECT * FROM lightbus_processed_events')


async def get_outbox():
    connection = await verification_connection()
    return await connection.fetch('SELECT * FROM lightbus_event_outbox')



@pytest.mark.run_loop
async def test_migrate(asyncpg_database: AsyncPostgresConnection):
    await asyncpg_database.migrate()
    # The following would fail if the tables didn't exist
    assert await total_processed_events() == 0
    assert await outbox_size() == 0


@pytest.mark.run_loop
async def test_transaction_start_commit(asyncpg_database: AsyncPostgresConnection):
    await asyncpg_database.migrate()
    transaction = await asyncpg_database.start_transaction()
    await asyncpg_database.store_processed_event(EventMessage(api_name='api', event_name='event', id='123'))
    await asyncpg_database.commit_transaction(transaction)
    assert await total_processed_events() == 1


@pytest.mark.run_loop
async def test_transaction_start_rollback(asyncpg_database: AsyncPostgresConnection):
    await asyncpg_database.migrate()
    transaction = await asyncpg_database.start_transaction()
    await asyncpg_database.store_processed_event(EventMessage(api_name='api', event_name='event', id='123'))
    await asyncpg_database.rollback_transaction(transaction)
    assert await total_processed_events() == 0


@pytest.mark.run_loop
async def test_transaction_start_rollback_continue(asyncpg_database: AsyncPostgresConnection):
    # Check we can still use the connection following a rollback
    await asyncpg_database.migrate()
    transaction = await asyncpg_database.start_transaction()
    await asyncpg_database.store_processed_event(EventMessage(api_name='api', event_name='event', id='123'))
    await asyncpg_database.rollback_transaction(transaction)  # Rollback
    await asyncpg_database.store_processed_event(EventMessage(api_name='api', event_name='event', id='123'))
    assert await total_processed_events() == 1


@pytest.mark.run_loop
async def test_is_event_duplicate_true(asyncpg_database: AsyncPostgresConnection):
    await asyncpg_database.migrate()
    message = EventMessage(api_name='api', event_name='event', id='123')
    await asyncpg_database.store_processed_event(message)
    assert await asyncpg_database.is_event_duplicate(message) == True


@pytest.mark.run_loop
async def test_is_event_duplicate_false(asyncpg_database: AsyncPostgresConnection):
    await asyncpg_database.migrate()
    message = EventMessage(api_name='api', event_name='event', id='123')
    assert await asyncpg_database.is_event_duplicate(message) == False


@pytest.mark.run_loop
async def test_send_event_ok(asyncpg_database: AsyncPostgresConnection):
    await asyncpg_database.migrate()
    message = EventMessage(api_name='api', event_name='event', kwargs={'field': 'abc'}, id='123')
    options = {'key': 'value'}
    await asyncpg_database.send_event(message, options)
    assert await outbox_size() == 1
    retrieved_message, options = await asyncpg_database.consume_pending_events(message_id='123').__anext__()
    assert retrieved_message.id == '123'
    assert retrieved_message.get_kwargs() == {'field': 'abc'}
    assert retrieved_message.get_metadata() == {}
