import pytest

import lightbus
from lightbus import BusNode, TransactionalEventTransport
from lightbus.transports.transactional import lightbus_atomic, DbApiConnection
from lightbus.utilities.async import block

# Utility functions & fixtures
from tests.transactional_transport.conftest import verification_connection


@pytest.fixture()
def get_processed_events():

    async def inner():
        async with verification_connection() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT * FROM lightbus_processed_events")
                return await cursor.fetchall()

    return inner


@pytest.fixture()
def get_outbox():

    async def inner():
        async with verification_connection() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT * FROM lightbus_event_outbox")
                return await cursor.fetchall()

    return inner


@pytest.fixture()
def messages_in_redis(redis_client):

    async def inner(api_name, event_name):
        return await redis_client.xrange(f"{api_name}.{event_name}:stream")

    return inner


@pytest.fixture()
def transactional_bus(dummy_bus: BusNode, new_redis_pool, aiopg_connection, aiopg_cursor, loop):
    transport = TransactionalEventTransport(
        child_transport=lightbus.RedisEventTransport(
            redis_pool=new_redis_pool(maxsize=10000),
            consumer_group_prefix="test_cg",
            consumer_name="test_consumer",
        )
    )
    registry = dummy_bus.bus_client.transport_registry
    registry.set_event_transport("default", transport)

    database = DbApiConnection(aiopg_connection, aiopg_cursor)
    block(database.migrate(), loop=loop, timeout=1)
    block(aiopg_cursor.execute("COMMIT"), loop=loop, timeout=1)
    block(aiopg_cursor.execute("BEGIN"), loop=loop, timeout=1)

    return dummy_bus


@pytest.yield_fixture()
def test_table(aiopg_cursor, loop):
    block(aiopg_cursor.execute("DROP TABLE IF EXISTS test_table"), loop=loop, timeout=1)
    block(aiopg_cursor.execute("CREATE TABLE test_table (pk VARCHAR(100))"), loop=loop, timeout=1)
    block(aiopg_cursor.execute("COMMIT"), loop=loop, timeout=1)
    block(aiopg_cursor.execute("BEGIN"), loop=loop, timeout=1)

    class TestTable(object):

        async def total_rows(self):
            await aiopg_cursor.execute("SELECT COUNT(*) FROM test_table")
            return (await aiopg_cursor.fetchone())[0]

    yield TestTable()

    block(aiopg_cursor.execute("DROP TABLE test_table"), loop=loop, timeout=1)
    block(aiopg_cursor.execute("COMMIT"), loop=loop, timeout=1)
    block(aiopg_cursor.execute("BEGIN"), loop=loop, timeout=1)


# Tests


@pytest.mark.run_loop
async def test_fire_events_all_ok(
    transactional_bus,
    aiopg_connection,
    test_table,
    dummy_api,
    aiopg_cursor,
    messages_in_redis,
    get_outbox,
):
    async with lightbus_atomic(transactional_bus, aiopg_connection, apis=["foo", "bar"]):
        await transactional_bus.my.dummy.my_event.fire_async(field=1)
        await aiopg_cursor.execute("INSERT INTO test_table VALUES ('hey')")

    assert await test_table.total_rows() == 1
    assert len(await get_outbox()) == 0  # Sent messages are removed from outbox
    assert len(await messages_in_redis("my.dummy", "my_event")) == 1


@pytest.mark.run_loop
async def test_fire_events_exception(
    transactional_bus,
    aiopg_connection,
    test_table,
    dummy_api,
    aiopg_cursor,
    messages_in_redis,
    get_outbox,
):

    class OhNo(Exception):
        pass

    with pytest.raises(OhNo):
        async with lightbus_atomic(transactional_bus, aiopg_connection, apis=["foo", "bar"]):
            await transactional_bus.my.dummy.my_event.fire_async(field=1)
            await aiopg_cursor.execute("INSERT INTO test_table VALUES ('hey')")
            raise OhNo()

    assert await test_table.total_rows() == 0
    assert len(await get_outbox()) == 0
    assert len(await messages_in_redis("my.dummy", "my_event")) == 0
