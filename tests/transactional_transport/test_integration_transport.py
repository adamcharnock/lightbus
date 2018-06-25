import asyncio

import pytest

from lightbus import BusPath
from lightbus.transports.transactional import lightbus_set_database
from lightbus.utilities.async import cancel

pytestmark = pytest.mark.integration


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
    async with lightbus_set_database(transactional_bus, aiopg_connection, apis=["foo", "bar"]):
        await transactional_bus.my.dummy.my_event.fire_async(field=1)
        await aiopg_cursor.execute("INSERT INTO test_table VALUES ('hey')")

    assert await test_table.total_rows() == 1  # Test table has data
    assert len(await get_outbox()) == 0  # Sent messages are removed from outbox
    assert len(await messages_in_redis("my.dummy", "my_event")) == 1  # Message in redis


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
        async with lightbus_set_database(transactional_bus, aiopg_connection, apis=["foo", "bar"]):
            await transactional_bus.my.dummy.my_event.fire_async(field=1)
            await aiopg_cursor.execute("INSERT INTO test_table VALUES ('hey')")
            raise OhNo()

    assert await test_table.total_rows() == 0  # No data in tests table
    assert len(await get_outbox()) == 0  # No messages sat in outbox
    assert len(await messages_in_redis("my.dummy", "my_event")) == 0  # Nothing in redis


@pytest.mark.run_loop
async def test_consume_events(transactional_bus: BusPath, aiopg_connection_factory, dummy_api):
    events = []

    def listener(event_message, *, field):
        events.append(event_message)

    connection1 = await aiopg_connection_factory()
    async with lightbus_set_database(transactional_bus, connection1, apis=["my.dummy"]):
        listener_co = await transactional_bus.my.dummy.my_event.listen_async(listener)

    task = asyncio.ensure_future(listener_co)
    await asyncio.sleep(0.2)

    connection2 = await aiopg_connection_factory()

    print("connection 1", connection1)
    print("connection 2", connection2)

    async with lightbus_set_database(transactional_bus, connection2, apis=["my.dummy"]):
        await transactional_bus.my.dummy.my_event.fire_async(field=1)

    await asyncio.sleep(0.2)

    await cancel(task)

    assert len(events) == 1
