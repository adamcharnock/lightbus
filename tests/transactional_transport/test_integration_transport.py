import pytest

import lightbus
from lightbus import BusNode, TransactionalEventTransport
from lightbus.transports.transactional import lightbus_atomic, DbApiConnection
from lightbus.utilities.async import block

from tests.transactional_transport.conftest import verification_connection


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
        async with lightbus_atomic(transactional_bus, aiopg_connection, apis=["foo", "bar"]):
            await transactional_bus.my.dummy.my_event.fire_async(field=1)
            await aiopg_cursor.execute("INSERT INTO test_table VALUES ('hey')")
            raise OhNo()

    assert await test_table.total_rows() == 0  # No data in tests table
    assert len(await get_outbox()) == 0  # No messages sat in outbox
    assert len(await messages_in_redis("my.dummy", "my_event")) == 0  # Nothing in redis
