""" Test each plugin hook is called at the correct time

"""
import pytest

from lightbus import BusNode, BusClient
from lightbus.message import RpcMessage

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


def test_server_start_stop(mocker, called_hooks, dummy_bus: BusNode, loop):
    mocker.patch.object(BusClient, '_run_forever')
    dummy_bus.run_forever(loop=loop)
    assert called_hooks() == ['before_server_start', 'after_server_stopped']


def test_rpc_calls(called_hooks, dummy_bus: BusNode, loop):
    dummy_bus.my.dummy.my_proc()
    assert called_hooks() == ['before_rpc_call', 'after_rpc_call']

