""" Test each plugin hook is called at the correct time

"""
import asyncio
import pytest

from lightbus import BusClient
from lightbus.path import BusPath
from lightbus.message import RpcMessage, EventMessage
from lightbus.plugins import LightbusPlugin

pytestmark = pytest.mark.unit


@pytest.fixture
def called_hooks(mocker, dummy_bus: BusPath):
    # Patch only applies to module in which execute_hook is used, not
    # where it is defined
    async def dummy_coroutine(*args, **kwargs):
        pass

    m = mocker.patch.object(
        dummy_bus.client.plugin_registry, "execute_hook", side_effect=dummy_coroutine
    )
    return lambda: [kwargs.get("name") or args[0] for args, kwargs in m.call_args_list]


@pytest.fixture
def add_base_plugin(dummy_bus: BusPath):
    # Add the base plugin so that the plugins framework has something to call
    # None of the base plugin's methods do anything, but it allows our
    # called_hooks() fixture above to detch the call
    def do_add_base_plugin():
        dummy_bus.client.plugin_registry.set_plugins([LightbusPlugin()])

    return do_add_base_plugin


def test_server_start_stop(mocker, called_hooks, dummy_bus: BusPath, add_base_plugin, dummy_api):
    add_base_plugin()
    dummy_bus.client.register_api(dummy_api)
    mocker.patch.object(BusClient, "_actually_run_forever")
    dummy_bus.client.run_forever()
    assert called_hooks() == ["before_server_start", "after_server_stopped"]


def test_rpc_calls(called_hooks, dummy_bus: BusPath, loop, add_base_plugin, dummy_api):
    add_base_plugin()
    dummy_bus.client.register_api(dummy_api)
    dummy_bus.my.dummy.my_proc(field=123)
    assert called_hooks() == ["before_rpc_call", "after_rpc_call"]


@pytest.mark.asyncio
async def test_rpc_execution(
    called_hooks, dummy_bus: BusPath, loop, mocker, add_base_plugin, dummy_api
):
    class StopIt(Exception):
        pass

    add_base_plugin()
    await dummy_bus.client.register_api_async(dummy_api)

    async def dummy_transport_consume_rpcs(*args, **kwargs):
        if m.call_count == 1:
            return [
                RpcMessage(api_name="my.dummy", procedure_name="my_proc", kwargs={"field": 123})
            ]
        else:
            raise StopIt()

    rpc_transport = dummy_bus.client.transport_registry.get_rpc_transport("default")
    m = mocker.patch.object(rpc_transport, "consume_rpcs", side_effect=dummy_transport_consume_rpcs)

    try:
        await dummy_bus.client.consume_rpcs()
    except StopIt:  # Gross. Need to escape the infinite loop in client.consume_rpcs() somehow
        pass

    assert called_hooks() == ["before_rpc_execution", "after_rpc_execution"]


def test_event_sent(called_hooks, dummy_bus: BusPath, loop, add_base_plugin, dummy_api):
    add_base_plugin()
    dummy_bus.client.register_api(dummy_api)
    dummy_bus.my.dummy.my_event.fire(field="foo")
    assert called_hooks() == ["before_event_sent", "after_event_sent"]


@pytest.mark.asyncio
async def test_event_execution(called_hooks, dummy_bus: BusPath, loop, add_base_plugin, dummy_api):
    add_base_plugin()
    await dummy_bus.client.register_api_async(dummy_api)

    task = await dummy_bus.client.listen_for_event(
        "my.dummy", "my_event", lambda *a, **kw: None, listener_name="test"
    )
    await asyncio.sleep(0.1)

    # Send the event message using a lower-level API to avoid triggering the
    # before_event_sent & after_event_sent plugin hooks. We don't care about those here
    event_message = EventMessage(api_name="my.dummy", event_name="my_event", kwargs={"field": 1})
    event_transport = dummy_bus.client.transport_registry.get_event_transport("default")
    await event_transport.send_event(event_message, options={})
    await asyncio.sleep(0.1)

    assert called_hooks() == ["before_event_execution", "after_event_execution"]
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
