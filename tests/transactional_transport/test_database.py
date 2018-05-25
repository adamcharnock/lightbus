import asyncpg
import pytest

from lightbus.transports.transactional import AsyncPostgresConnection
from tests.transactional_transport.conftest import verification_connection


async def total_processed_events():
    connection = await verification_connection()
    return await connection.fetchval('SELECT COUNT(*) FROM lightbus_processed_events')


async def outbox_size():
    connection = await verification_connection()
    return await connection.fetchval('SELECT COUNT(*) FROM lightbus_processed_events')


@pytest.mark.run_loop
async def test_migrate(asyncpg_database: AsyncPostgresConnection):
    await asyncpg_database.migrate()
    # The following would fail if the tables didn't exist
    assert await total_processed_events() == 0
    assert await outbox_size() == 0
