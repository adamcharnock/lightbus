import asyncio

import logging
import pytest
import lightbus
from tests.dummy_api import DummyApi


# Should actually test events for reliability. We want events to
# be delivered AT LEAST ONCE. RPCs don't matter so much because
# if they fail the caller will know about it.
@pytest.mark.run_loop
@pytest.mark.skip
async def test_rpc(bus: lightbus.BusNode, mocker, dummy_api, caplog):
    """Full rpc call integration test"""
    caplog.set_level(logging.WARNING)

    responses = set()
    async def co_call_rpc():
        asyncio.sleep(1)
        for x in range(0, 10):
            responses.add(
                await bus.my.dummy.sudden_death.call_async(n=x)
            )

    async def co_consume_rpcs():
        await bus.bus_client.consume_rpcs(apis=[dummy_api])

    (call_task, ), (consume_task, ) = await asyncio.wait([co_call_rpc(), co_consume_rpcs()], return_when=asyncio.FIRST_COMPLETED)
    consume_task.cancel()

    assert len(responses) == 10
    assert set(range(0, 10)) == {int(n) for n in responses}
