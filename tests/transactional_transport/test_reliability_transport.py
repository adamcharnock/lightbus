import asyncio
import logging

import pytest

from lightbus.transports.transactional import lightbus_atomic


@pytest.mark.run_loop
async def test_multiple_connections(
    transactional_bus,  # Ensure migrations get run
    transactional_bus_factory,
    pg_kwargs,
    test_table,
    loop,
    dummy_api,
    # aiopg_cursor,
    messages_in_redis,
    get_outbox,
    caplog,
):
    import aiopg

    caplog.set_level(logging.WARNING)

    async def start_firing(number):

        async with aiopg.connect(loop=loop, **pg_kwargs) as connection:
            async with connection.cursor() as cursor:
                bus = await transactional_bus_factory()

                for x in range(0, 50):
                    async with lightbus_atomic(bus, connection, apis=["my.dummy"]):
                        await bus.my.dummy.my_event.fire_async(field=1)
                        await cursor.execute(
                            "INSERT INTO test_table VALUES (%s)", [f"{number}-{x}"]
                        )

    await asyncio.gather(*[start_firing(n) for n in range(0, 5)])

    assert await test_table.total_rows() == 250
    assert len(await get_outbox()) == 0
    assert len(await messages_in_redis("my.dummy", "my_event")) == 250
