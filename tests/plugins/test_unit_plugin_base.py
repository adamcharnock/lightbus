from collections import OrderedDict

from lightbus.plugins import get_plugins, manually_set_plugins, LightbusPlugin, autoload_plugins, plugin_hook, \
    remove_all_plugins
from lightbus.plugins.state import StatePlugin


def test_manually_set_plugins():
    assert get_plugins() == []
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
    assert get_plugins() == []
    assert autoload_plugins()
    assert [(name, p.__class__) for name, p in get_plugins().items()] == [
        ('foo', StatePlugin),
    ]


def test_plugin_hook(mocker):
    """Ensure calling plugin_hook() calls the method on the plugin"""
    assert get_plugins() == []
    plugin = LightbusPlugin()
    manually_set_plugins(OrderedDict([
        ('p1', plugin),
    ]))
    m = mocker.patch.object(plugin, 'before_server_start')
    plugin_hook('before_server_start', bus_client=None, loop=None)
    assert m.called


def test_remove_all_plugins():
    assert get_plugins() == []
    manually_set_plugins(OrderedDict([
        ('p1', LightbusPlugin()),
    ]))
    remove_all_plugins()
    assert get_plugins() == []
