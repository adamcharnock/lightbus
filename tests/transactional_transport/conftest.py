import asyncio
import os
import urllib.parse
from copy import copy
from typing import AsyncGenerator, Awaitable

import psycopg2
from psycopg2.extensions import cursor
import pytest

import lightbus
import lightbus.path
from lightbus import TransactionalEventTransport
from lightbus.path import BusPath
from lightbus.transports.base import TransportRegistry
from lightbus.transports.redis import StreamUse
from lightbus.transports.transactional import DbApiConnection
from lightbus.utilities.async import block

if False:
    import aiopg


@pytest.fixture()
def cursor_factory():

    class ErrorThrowingCursor(cursor):

        def __init__(self, conn, *args, **kwargs):
            self.conn = conn
            super().__init__(conn, *args, **kwargs)

        def execute(self, query, vars=None):
            result = super().execute(query, vars)

            for notice in self.conn.notices:
                level, message = notice.split(": ")

                command = query.upper().split(" ")[0]
                if level == "WARNING":
                    raise psycopg2.Warning(
                        f"Postgres issued a warning. The best way to check "
                        f"what is going on is to set log_statement=all in postgres "
                        f"and view the output (this is how the tests/docker-compose.yaml "
                        f"file is setup already). However, it is POSSIBLE this query may have "
                        f"been at fault: {message.strip()}"
                    )

            return result

    return ErrorThrowingCursor


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

    connection = block(aiopg.connect(**pg_kwargs), timeout=2)
    yield connection
    connection.close()


@pytest.yield_fixture()
def aiopg_connection_factory(pg_kwargs, loop):
    import aiopg

    connections = []

    async def factory():
        connection = await aiopg.connect(loop=loop, **pg_kwargs)
        connections.append(connection)
        return connection

    yield factory

    for connection in connections:
        connection.close()


@pytest.yield_fixture()
def psycopg2_connection(pg_kwargs, loop):
    import psycopg2

    connection = psycopg2.connect(**pg_kwargs)
    yield connection
    connection.close()


@pytest.fixture()
def aiopg_cursor(aiopg_connection, loop, cursor_factory):
    cursor = block(aiopg_connection.cursor(cursor_factory=cursor_factory), timeout=1)
    block(cursor.execute("BEGIN -- aiopg_cursor"), timeout=1)
    block(cursor.execute("DROP TABLE IF EXISTS lightbus_processed_events"), timeout=1)
    block(cursor.execute("DROP TABLE IF EXISTS lightbus_event_outbox"), timeout=1)
    block(cursor.execute("COMMIT -- aiopg_cursor"), timeout=1)
    return cursor


@pytest.fixture()
def dbapi_database(aiopg_connection, aiopg_cursor, loop):
    return DbApiConnection(aiopg_connection, aiopg_cursor)


def verification_connection() -> Awaitable["aiopg.Connection"]:
    import aiopg

    return aiopg.connect(**pg_kwargs(pg_url()), loop=asyncio.get_event_loop())


@pytest.fixture()
def get_outbox():

    async def inner():
        async with verification_connection() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT * FROM lightbus_event_outbox")
                return await cursor.fetchall()

    return inner


@pytest.fixture()
def get_processed_events():

    async def inner():
        async with verification_connection() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT * FROM lightbus_processed_events")
                return await cursor.fetchall()

    return inner


@pytest.fixture()
def messages_in_redis(redis_client):

    async def inner(api_name, event_name):
        return await redis_client.xrange(f"{api_name}.{event_name}:stream")

    return inner


@pytest.fixture()
def transactional_bus_factory(dummy_bus: BusPath, new_redis_pool, loop):
    pool = new_redis_pool(maxsize=10000)

    async def inner():
        transport = TransactionalEventTransport(
            child_transport=lightbus.RedisEventTransport(
                redis_pool=pool,
                consumer_group_prefix="test_cg",
                consumer_name="test_consumer",
                stream_use=StreamUse.PER_EVENT,
            )
        )
        config = dummy_bus.client.config
        transport_registry = TransportRegistry().load_config(config)
        transport_registry.set_event_transport("default", transport)
        client = lightbus.BusClient(config=config, transport_registry=transport_registry)
        bus = lightbus.path.BusPath(name="", parent=None, client=client)
        return bus

    return inner


@pytest.fixture()
def transactional_bus(dummy_bus: BusPath, new_redis_pool, aiopg_connection, aiopg_cursor, loop):
    transport = TransactionalEventTransport(
        child_transport=lightbus.RedisEventTransport(
            redis_pool=new_redis_pool(maxsize=10000),
            consumer_group_prefix="test_cg",
            consumer_name="test_consumer",
            stream_use=StreamUse.PER_EVENT,
        )
    )
    registry = dummy_bus.client.transport_registry
    registry.set_event_transport("default", transport)

    database = DbApiConnection(aiopg_connection, aiopg_cursor)
    # Don't migrate here, that should be handled by the auto-migration

    return dummy_bus


@pytest.yield_fixture()
def test_table(aiopg_cursor, loop):
    block(aiopg_cursor.execute("BEGIN -- test_table (setup)"), timeout=1)
    block(aiopg_cursor.execute("DROP TABLE IF EXISTS test_table"), timeout=1)
    block(aiopg_cursor.execute("CREATE TABLE test_table (pk VARCHAR(100))"), timeout=1)
    block(aiopg_cursor.execute("COMMIT -- test_table (setup)"), timeout=1)

    class TestTable(object):

        async def total_rows(self):
            await aiopg_cursor.execute("SELECT COUNT(*) FROM test_table")
            return (await aiopg_cursor.fetchone())[0]

    yield TestTable()

    block(aiopg_cursor.execute("BEGIN -- test_table (tear down)"), timeout=1)
    block(aiopg_cursor.execute("DROP TABLE test_table"), timeout=1)
    block(aiopg_cursor.execute("COMMIT -- test_table (tear down)"), timeout=1)
