"""Test the infrastructure for loading and calling plugins"""
import pytest
from collections import OrderedDict

from lightbus.config import Config
from lightbus.plugins import get_plugins, manually_set_plugins, LightbusPlugin, autoload_plugins, plugin_hook, \
    remove_all_plugins, is_plugin_loaded
from lightbus.plugins.metrics import MetricsPlugin
from lightbus.plugins.state import StatePlugin


pytestmark = pytest.mark.unit


def test_manually_set_plugins():
    assert get_plugins() is None
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
    config = Config.load_dict({})
    assert get_plugins() is None
    assert autoload_plugins(config)
    assert [(name, p.__class__) for name, p in get_plugins().items()] == [
        ('internal_state', StatePlugin),
        ('internal_metrics', MetricsPlugin),
    ]


@pytest.mark.run_loop
async def test_plugin_hook(mocker):
    """Ensure calling plugin_hook() calls the method on the plugin"""
    assert get_plugins() is None
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
    assert get_plugins() is None
    manually_set_plugins(OrderedDict([
        ('p1', LightbusPlugin()),
    ]))
    remove_all_plugins()
    assert get_plugins() is None


def test_is_plugin_loaded():
    assert get_plugins() is None
    assert is_plugin_loaded(LightbusPlugin) == False
    manually_set_plugins(OrderedDict([
        ('p1', LightbusPlugin()),
    ]))
    assert is_plugin_loaded(LightbusPlugin) == True


def test_plugin_config():
    # Is the Config attached to the plugin class by the
    # base plugin's metaclass?
    class PluginWithConfig(LightbusPlugin):
        @classmethod
        def from_config(cls, first: int=123):
            pass

    assert PluginWithConfig.Config
    assert type(PluginWithConfig.Config) == type
    assert 'first' in PluginWithConfig.Config.__annotations__
    assert PluginWithConfig.Config().first == 123
