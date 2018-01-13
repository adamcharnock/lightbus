""" Test each plugin hook is called at the correct time

"""
import pytest

from lightbus import BusNode, BusClient
from lightbus.message import RpcMessage
from lightbus.plugins import manually_set_plugins, LightbusPlugin

pytestmark = pytest.mark.unit


@pytest.fixture
def called_hooks(mocker):
    # Patch only applies to module in which plugin_hook is used, not
    # where it is defined
    async def dummy_coroutine(*args, **kwargs):
        pass
    m = mocker.patch('lightbus.bus.plugin_hook', side_effect=dummy_coroutine)
    return lambda: [
        kwargs.get('name') or args[0]
        for args, kwargs
        in m.call_args_list
    ]


@pytest.fixture
def add_base_plugin():
    # Add the base plugin so that the plugins framework has something to call
    # None of the base plugin's methods do anything, but it allows our
    # called_hooks() fixture above to detch the call
    def do_add_base_plugin():
        manually_set_plugins(plugins={'base': LightbusPlugin()})
    return do_add_base_plugin


def test_server_start_stop(mocker, called_hooks, dummy_bus: BusNode, loop, add_base_plugin, dummy_api):
    add_base_plugin()
    mocker.patch.object(BusClient, '_run_forever')
    dummy_bus.run_forever(loop=loop)
    assert called_hooks() == ['before_server_start', 'after_server_stopped']


def test_rpc_calls(called_hooks, dummy_bus: BusNode, loop, add_base_plugin, dummy_api):
    add_base_plugin()
    dummy_bus.my.dummy.my_proc()
    assert called_hooks() == ['before_rpc_call', 'after_rpc_call']


@pytest.mark.run_loop
async def test_rpc_execution(called_hooks, dummy_bus: BusNode, loop, mocker, add_base_plugin, dummy_api):
    class StopIt(Exception): pass

    add_base_plugin()
    async def dummy_transport_consume_rpcs(*args, **kwargs):
        if m.call_count == 1:
            return [RpcMessage(
                api_name='my.dummy',
                procedure_name='my_proc',
                kwargs={'field': 123},
            )]
        else:
            raise StopIt()

    m = mocker.patch.object(dummy_bus.bus_client.rpc_transport, 'consume_rpcs', side_effect=dummy_transport_consume_rpcs)

    try:
        await dummy_bus.bus_client.consume_rpcs()
    except StopIt:  # Gross. Need to escape the infinite loop in bus_client.consume_rpcs() somehow
        pass

    assert called_hooks() == ['before_rpc_execution', 'after_rpc_execution']


def test_event_sent(called_hooks, dummy_bus: BusNode, loop, add_base_plugin, dummy_api):
    add_base_plugin()
    dummy_bus.my.dummy.my_event.fire(field='foo')
    assert called_hooks() == ['before_event_sent', 'after_event_sent']


@pytest.mark.run_loop
async def test_event_execution(called_hooks, dummy_bus: BusNode, loop, add_base_plugin, dummy_api):
    add_base_plugin()
    await dummy_bus.bus_client._consume_events_once()
    assert called_hooks() == ['before_event_execution', 'after_event_execution']
