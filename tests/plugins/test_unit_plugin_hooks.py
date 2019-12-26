""" Test each plugin hook is called at the correct time

"""
import asyncio
from contextlib import contextmanager

import pytest

from lightbus import BusClient
from lightbus.path import BusPath
from lightbus.message import RpcMessage, EventMessage
from lightbus.plugins import LightbusPlugin
from tests.conftest import Worker

pytestmark = pytest.mark.unit


@pytest.fixture
def track_called_hooks(mocker):
    # Patch only applies to module in which execute_hook is used, not
    # where it is defined
    async def dummy_coroutine(*args, **kwargs):
        pass

    def inner(bus_client: BusClient):
        m = mocker.patch.object(bus_client.hook_registry, "execute", side_effect=dummy_coroutine)
        return lambda: [kwargs.get("name") or args[0] for args, kwargs in m.call_args_list]

    return inner


@pytest.fixture
def add_base_plugin(dummy_bus: BusPath):
    # Add the base plugin so that the plugins framework has something to call
    # None of the base plugin's methods do anything, but it allows our
    # called_hooks() fixture above to detch the call
    def do_add_base_plugin():
        dummy_bus.client.plugin_registry.set_plugins([LightbusPlugin()])

    return do_add_base_plugin


def test_server_start_stop(
    mocker, track_called_hooks, dummy_bus: BusPath, add_base_plugin, dummy_api
):
    add_base_plugin()
    dummy_bus.client.register_api(dummy_api)
    hook_tracker = track_called_hooks(dummy_bus.client)
    mocker.patch.object(BusClient, "_actually_run_forever")
    dummy_bus.client.run_forever()
    assert hook_tracker() == ["before_worker_start", "after_worker_stopped"]


def test_rpc_calls(track_called_hooks, dummy_bus: BusPath, loop, add_base_plugin, dummy_api):
    add_base_plugin()
    dummy_bus.client.register_api(dummy_api)
    hook_tracker = track_called_hooks(dummy_bus.client)
    dummy_bus.my.dummy.my_proc(field=123)
    assert hook_tracker() == ["before_rpc_call", "after_rpc_call"]


@pytest.mark.asyncio
async def test_rpc_execution(
    dummy_bus: BusPath, track_called_hooks, mocker, add_base_plugin, dummy_api
):
    add_base_plugin()
    dummy_bus.client.register_api(dummy_api)
    hook_tracker = track_called_hooks(dummy_bus.client)

    async def dummy_transport_consume_rpcs(*args, **kwargs):
        if m.call_count == 1:
            return [
                RpcMessage(api_name="my.dummy", procedure_name="my_proc", kwargs={"field": 123})
            ]
        else:
            await asyncio.sleep(1)
            return []

    rpc_transport = dummy_bus.client.transport_registry.get_rpc_transport("default")
    m = mocker.patch.object(rpc_transport, "consume_rpcs", side_effect=dummy_transport_consume_rpcs)

    await dummy_bus.client.consume_rpcs()
    # Give the bus worker a moment to execute the hooks
    await asyncio.sleep(0.01)

    assert hook_tracker() == ["before_rpc_execution", "after_rpc_execution"]


def test_event_sent(track_called_hooks, dummy_bus: BusPath, loop, add_base_plugin, dummy_api):
    add_base_plugin()
    dummy_bus.client.register_api(dummy_api)
    hook_tracker = track_called_hooks(dummy_bus.client)
    dummy_bus.my.dummy.my_event.fire(field="foo")
    assert hook_tracker() == ["before_event_sent", "after_event_sent"]


@pytest.mark.asyncio
async def test_event_execution(
    track_called_hooks, new_bus, worker: Worker, loop, add_base_plugin, dummy_api
):
    add_base_plugin()
    bus = new_bus()
    bus.client.register_api(dummy_api)
    hook_tracker = track_called_hooks(bus.client)

    bus.client.listen_for_event("my.dummy", "my_event", lambda *a, **kw: None, listener_name="test")

    async with worker(bus):
        await asyncio.sleep(0.1)

        # Send the event message using a lower-level API to avoid triggering the
        # before_event_sent & after_event_sent plugin hooks. We don't care about those here
        event_message = EventMessage(
            api_name="my.dummy", event_name="my_event", kwargs={"field": "a"}
        )
        event_transport = bus.client.transport_registry.get_event_transport("default")
        await event_transport.send_event(event_message, options={})
        await asyncio.sleep(0.1)

    # FYI: There is a chance of events firing twice (because the dummy_bus keeps firing events),
    assert "before_event_execution" in hook_tracker()
    assert "after_event_execution" in hook_tracker()
