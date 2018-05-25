import asyncio
import os

import pytest

from lightbus.transports.transactional import AsyncPostgresConnection
from lightbus.utilities.async import block

if False:
    import asyncpg


@pytest.fixture()
def pg_url():
    return os.environ.get('PG_URL', 'postgres://postgres@localhost:5432/postgres')


@pytest.fixture()
def asyncpg_connection(pg_url, loop):
    import asyncpg
    return block(asyncpg.connect(pg_url, loop=loop), loop=loop, timeout=2)


@pytest.fixture()
def asyncpg_database(asyncpg_connection):
    return AsyncPostgresConnection(connection=asyncpg_connection)


async def verification_connection() -> 'asyncpg.Connection':
    import asyncpg
    return await asyncpg.connect(pg_url(), loop=asyncio.get_event_loop())
