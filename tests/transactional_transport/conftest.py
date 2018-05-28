import asyncio
import os
import urllib.parse
from typing import AsyncGenerator, Awaitable

import pytest

from lightbus.transports.transactional import DbApiConnection
from lightbus.utilities.async import block

if False:
    import aiopg


@pytest.fixture()
def pg_url():
    return os.environ.get("PG_URL", "postgres://postgres@localhost:5432/postgres")


@pytest.fixture()
def pg_kwargs(pg_url):
    parsed = urllib.parse.urlparse(pg_url)
    assert parsed.scheme == "postgres"
    return {
        "dbname": parsed.path.strip("/") or "postgres",
        "user": parsed.username or "postgres",
        "password": parsed.password,
        "host": parsed.hostname,
        "port": parsed.port or 5432,
    }


@pytest.yield_fixture()
def aiopg_connection(pg_kwargs, loop):
    import aiopg

    connection = block(aiopg.connect(loop=loop, **pg_kwargs), loop=loop, timeout=2)
    yield connection
    connection.close()


@pytest.yield_fixture()
def psycopg2_connection(pg_kwargs, loop):
    import psycopg2

    connection = psycopg2.connect(**pg_kwargs)
    yield connection
    connection.close()


@pytest.fixture()
def aiopg_cursor(aiopg_connection, loop):
    cursor = block(aiopg_connection.cursor(), loop=loop, timeout=1)
    block(cursor.execute("DROP TABLE IF EXISTS lightbus_processed_events"), loop=loop, timeout=1)
    block(cursor.execute("DROP TABLE IF EXISTS lightbus_event_outbox"), loop=loop, timeout=1)
    return cursor


@pytest.fixture()
def dbapi_database(aiopg_connection, aiopg_cursor, loop):
    return DbApiConnection(aiopg_connection, aiopg_cursor)


def verification_connection() -> Awaitable["aiopg.Connection"]:
    import aiopg

    return aiopg.connect(**pg_kwargs(pg_url()), loop=asyncio.get_event_loop())
