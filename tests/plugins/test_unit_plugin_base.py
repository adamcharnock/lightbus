import pytest
from collections import OrderedDict

from lightbus.plugins import get_plugins, manually_set_plugins, LightbusPlugin, autoload_plugins, plugin_hook, \
    remove_all_plugins, is_plugin_loaded
from lightbus.plugins.state import StatePlugin


pytestmark = pytest.mark.unit


def test_manually_set_plugins():
    assert get_plugins() == OrderedDict()
    p1 = LightbusPlugin()
    p2 = LightbusPlugin()
    manually_set_plugins(OrderedDict([
        ('p1', p1),
        ('p2', p2),
    ]))
    assert get_plugins() == OrderedDict([
        ('p1', p1),
        ('p2', p2),
    ])


def test_autoload_plugins():
    assert get_plugins() == OrderedDict()
    assert autoload_plugins()
    assert [(name, p.__class__) for name, p in get_plugins().items()] == [
        ('foo', StatePlugin),
    ]


@pytest.mark.run_loop
async def test_plugin_hook(mocker):
    """Ensure calling plugin_hook() calls the method on the plugin"""
    assert get_plugins() == OrderedDict()
    plugin = LightbusPlugin()
    manually_set_plugins(OrderedDict([
        ('p1', plugin),
    ]))

    async def dummy_coroutine(*args, **kwargs):
        pass
    m = mocker.patch.object(plugin, 'before_server_start', return_value=dummy_coroutine())

    await plugin_hook('before_server_start', bus_client=None, loop=None)
    assert m.called


def test_remove_all_plugins():
    assert get_plugins() == OrderedDict()
    manually_set_plugins(OrderedDict([
        ('p1', LightbusPlugin()),
    ]))
    remove_all_plugins()
    assert get_plugins() == OrderedDict()


def test_is_plugin_loaded():
    assert get_plugins() == OrderedDict()
    assert is_plugin_loaded(LightbusPlugin) == False
    manually_set_plugins(OrderedDict([
        ('p1', LightbusPlugin()),
    ]))
    assert is_plugin_loaded(LightbusPlugin) == True
