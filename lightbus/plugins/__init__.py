from asyncio import AbstractEventLoop
from typing import Sequence, Dict

from collections import OrderedDict

import pkg_resources
import lightbus
from lightbus.exceptions import PluginsNotLoaded, PluginHookNotFound

_plugins = None
_hooks_names = []
ENTRYPOINT_NAME = 'lightbus_plugins'


# TODO: Document plugins in docs (and reference those docs here)
class LightbusPlugin(object):
    priority = 1000

    def __str__(self):
        return '{}.{}'.format(self.__class__.__module__, self.__class__.__name__)

    def before_server_start(self, *, bus_client: 'lightbus.bus.BusClient', loop: AbstractEventLoop):
        pass

    def after_server_stopped(self, *, bus_client: 'lightbus.bus.BusClient', loop: AbstractEventLoop):
        pass


def autoload_plugins():
    global _plugins, _hooks_names
    load_hook_names()

    found_plugins = []
    for entrypoint in pkg_resources.iter_entry_points(ENTRYPOINT_NAME):
        plugin_class = entrypoint.load()
        plugin = plugin_class()
        found_plugins.append((plugin.priority, entrypoint.module_name, entrypoint.name, plugin))

        _plugins = OrderedDict()
    for priority, module_name, name, plugin in sorted(found_plugins):
        if name in _plugins:
            pass
        _plugins[name] = plugin

    return _plugins


def manually_set_plugins(plugins: Dict[str, LightbusPlugin]):
    global _plugins
    load_hook_names()
    _plugins = plugins


def load_hook_names():
    """Load a list of valid hook names"""
    global _hooks_names
    _hooks_names = [k for k in LightbusPlugin.__dict__ if not k.startswith('_')]


def remove_all_plugins():
    """Remove all plugins. Useful for testing"""
    global _plugins
    _plugins = []


def get_plugins() -> Dict[str, LightbusPlugin]:
    global _plugins
    return _plugins


def plugin_hook(name, **kwargs):
    global _plugins
    if _plugins is None:
        raise PluginsNotLoaded("You must call load_plugins() before calling plugin_hook().")
    if name not in _hooks_names:
        raise PluginHookNotFound("Plugin hook '{}' could not be found. Must be one of: {}".format(
            name,
            ', '.join(_hooks_names)
        ))

    return_values = []
    for plugin in _plugins.values():
        handler = getattr(plugin, name, None)
        if handler:
            return_values.append(
                handler(**kwargs)
            )
    return return_values
