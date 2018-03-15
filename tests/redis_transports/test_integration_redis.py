import asyncio

import pytest
import lightbus
from lightbus.exceptions import LightbusTimeout, LightbusServerError

pytestmark = pytest.mark.integration


@pytest.mark.run_loop
async def test_bus_fixture(bus: lightbus.BusNode):
    """Just sanity check the fixture"""
    rpc_transport = bus.bus_client.transport_registry.get_rpc_transport('default')
    result_transport = bus.bus_client.transport_registry.get_result_transport('default')
    event_transport = bus.bus_client.transport_registry.get_event_transport('default')

    connection_manager_rpc = await rpc_transport.connection_manager()
    connection_manager_result = await result_transport.connection_manager()
    connection_manager_event = await event_transport.connection_manager()

    with await connection_manager_rpc as redis_rpc, \
            await connection_manager_result as redis_result, \
            await connection_manager_event as redis_event:

        # Each transport should have its own connection
        assert redis_rpc is not redis_result
        assert redis_result is not redis_event
        assert redis_rpc is not redis_event

        # Check they are all looking at the same redis instance
        await redis_rpc.set('X', 1)
        assert await redis_result.get('X')
        assert await redis_rpc.get('X')

        info = await redis_rpc.info()
        # transports: rpc, result, event, schema
        assert int(info['clients']['connected_clients']) == 4


@pytest.mark.run_loop
async def test_rpc(bus: lightbus.BusNode, dummy_api):
    """Full rpc call integration test"""

    async def co_call_rpc():
        await asyncio.sleep(0.1)
        return await bus.my.dummy.my_proc.call_async(field='Hello! 😎')

    async def co_consume_rpcs():
        return await bus.bus_client.consume_rpcs(apis=[dummy_api])

    (call_task, ), (consume_task, ) = await asyncio.wait([co_call_rpc(), co_consume_rpcs()], return_when=asyncio.FIRST_COMPLETED)
    consume_task.cancel()
    assert call_task.result() == 'value: Hello! 😎'


@pytest.mark.run_loop
async def test_rpc_timeout(bus: lightbus.BusNode, dummy_api):
    """Full rpc call integration test"""

    async def co_call_rpc():
        await asyncio.sleep(0.1)
        return await bus.my.dummy.sudden_death.call_async(n=0)

    async def co_consume_rpcs():
        return await bus.bus_client.consume_rpcs(apis=[dummy_api])

    (call_task, ), (consume_task, ) = await asyncio.wait([co_call_rpc(), co_consume_rpcs()], return_when=asyncio.FIRST_COMPLETED)
    consume_task.cancel()
    with pytest.raises(LightbusTimeout):
        call_task.result()


@pytest.mark.run_loop
async def test_rpc_error(bus: lightbus.BusNode, dummy_api):
    """Test what happens when the remote procedure throws an error"""

    async def co_call_rpc():
        await asyncio.sleep(0.1)
        return await bus.my.dummy.general_error.call_async()

    async def co_consume_rpcs():
        return await bus.bus_client.consume_rpcs(apis=[dummy_api])

    (call_task, ), (consume_task, ) = await asyncio.wait([co_call_rpc(), co_consume_rpcs()], return_when=asyncio.FIRST_COMPLETED)

    consume_task.cancel()
    call_task.cancel()

    with pytest.raises(LightbusServerError):
        await call_task.result()


@pytest.mark.run_loop
async def test_event(bus: lightbus.BusNode, dummy_api):
    """Full event integration test"""

    received_kwargs = []

    async def listener(**kwargs):
        received_kwargs.append(kwargs)

    await bus.my.dummy.my_event.listen_async(listener)
    await asyncio.sleep(0.01)
    await bus.my.dummy.my_event.fire_async(field='Hello! 😎')

    # await asyncio.gather(co_fire_event(), co_listen_for_events())
    assert received_kwargs == [{'field': 'Hello! 😎'}]


@pytest.mark.run_loop
async def test_rpc_ids(bus: lightbus.BusNode, dummy_api, mocker):
    """Ensure the rpc_id comes back correctly"""

    async def co_call_rpc():
        await asyncio.sleep(0.1)
        return await bus.my.dummy.my_proc.call_async(field='foo')

    async def co_consume_rpcs():
        return await bus.bus_client.consume_rpcs(apis=[dummy_api])

    mocker.spy(bus.bus_client, 'send_result')

    (call_task, ), (consume_task, ) = await asyncio.wait([co_call_rpc(), co_consume_rpcs()], return_when=asyncio.FIRST_COMPLETED)
    _, kw = bus.bus_client.send_result.call_args
    rpc_message = kw['rpc_message']
    result_message = kw['result_message']
    consume_task.cancel()

    assert rpc_message.rpc_id
    assert result_message.rpc_id
    assert rpc_message.rpc_id == result_message.rpc_id


def test_multiple_rpc_transports():
    pass


def test_multiple_event_transports():
    pass
