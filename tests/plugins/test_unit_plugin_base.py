"""Test the infrastructure for loading and calling plugins"""
import pytest
from collections import OrderedDict

from lightbus.config import Config
from lightbus.plugins import LightbusPlugin, PluginRegistry
from lightbus.plugins.metrics import MetricsPlugin
from lightbus.plugins.state import StatePlugin


pytestmark = pytest.mark.unit


def test_manually_set_plugins(plugin_registry: PluginRegistry):
    assert not plugin_registry._plugins
    p1 = LightbusPlugin()
    p2 = LightbusPlugin()
    plugin_registry.set_plugins([p1, p2])
    assert plugin_registry._plugins == [p1, p2]


def test_autoload_plugins(plugin_registry: PluginRegistry):
    config = Config.load_dict({})
    assert not plugin_registry._plugins
    assert plugin_registry.autoload_plugins(config)
    assert [type(p) for p in plugin_registry._plugins] == [StatePlugin, MetricsPlugin]


@pytest.mark.asyncio
async def test_execute_hook(mocker, plugin_registry: PluginRegistry):
    """Ensure calling execute_hook() calls the method on the plugin"""
    assert not plugin_registry._plugins
    plugin = LightbusPlugin()
    plugin_registry.set_plugins([plugin])

    async def dummy_coroutine(*args, **kwargs):
        pass

    m = mocker.patch.object(plugin, "before_server_start", return_value=dummy_coroutine())

    await plugin_registry.execute_hook("before_server_start", client=None, loop=None)
    assert m.called


def test_is_plugin_loaded(plugin_registry: PluginRegistry):
    assert plugin_registry.is_plugin_loaded(LightbusPlugin) == False
    plugin_registry.set_plugins([LightbusPlugin()])
    assert plugin_registry.is_plugin_loaded(LightbusPlugin) == True


def test_plugin_config():
    # Is the Config attached to the plugin class by the
    # base plugin's metaclass?
    class PluginWithConfig(LightbusPlugin):
        @classmethod
        def from_config(cls, config, first: int = 123):
            pass

    assert PluginWithConfig.Config
    assert type(PluginWithConfig.Config) == type
    assert "config" not in PluginWithConfig.Config.__annotations__
    assert "first" in PluginWithConfig.Config.__annotations__
    assert PluginWithConfig.Config().first == 123
