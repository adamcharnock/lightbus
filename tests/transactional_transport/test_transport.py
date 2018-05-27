import asyncio

import pytest

from lightbus import TransactionalEventTransport, RedisEventTransport, DebugEventTransport
from lightbus.config import Config
from lightbus.transports.transactional.transport import ConnectionAlreadySet, DatabaseNotSet
from tests.transactional_transport.conftest import verification_connection


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
    await transport.set_connection(aiopg_connection, aiopg_cursor, start_transaction=True)
    assert transport.connection == aiopg_connection
    assert transport.cursor == aiopg_cursor
    assert await active_transactions() == 1


@pytest.mark.run_loop
async def test_set_connection_already_set(aiopg_connection, aiopg_cursor):
    transport = TransactionalEventTransport(DebugEventTransport())
    await transport.set_connection(aiopg_connection, aiopg_cursor, start_transaction=True)
    with pytest.raises(ConnectionAlreadySet):
        await transport.set_connection(aiopg_connection, aiopg_cursor, start_transaction=True)


@pytest.mark.run_loop
async def test_commit_and_finish_no_database():
    transport = TransactionalEventTransport(DebugEventTransport())
    with pytest.raises(DatabaseNotSet):
        await transport.commit_and_finish()


@pytest.mark.run_loop
async def test_commit_and_finish_ok(aiopg_connection, aiopg_cursor):
    transport = TransactionalEventTransport(DebugEventTransport())
    await transport.set_connection(aiopg_connection, aiopg_cursor, start_transaction=True)
    await transport.commit_and_finish()
    assert await active_transactions() == 0


@pytest.mark.run_loop
async def test_rollback_and_finish_ok(aiopg_connection, aiopg_cursor):
    transport = TransactionalEventTransport(DebugEventTransport())
    await transport.set_connection(aiopg_connection, aiopg_cursor, start_transaction=True)
    await transport.rollback_and_finish()
    assert await active_transactions() == 0
