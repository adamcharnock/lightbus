from asyncio import AbstractEventLoop
from typing import Sequence, Dict, Type

from collections import OrderedDict

import pkg_resources
import lightbus
from lightbus.exceptions import PluginsNotLoaded, PluginHookNotFound
from lightbus.message import RpcMessage, EventMessage, ResultMessage

_plugins = None
_hooks_names = []
ENTRYPOINT_NAME = 'lightbus_plugins'


# TODO: Document plugins in docs (and reference those docs here)
class LightbusPlugin(object):
    priority = 1000

    def __str__(self):
        return '{}.{}'.format(self.__class__.__module__, self.__class__.__name__)

    async def before_server_start(self, *, bus_client: 'lightbus.bus.BusClient', loop: AbstractEventLoop):
        pass

    async def after_server_stopped(self, *, bus_client: 'lightbus.bus.BusClient', loop: AbstractEventLoop):
        pass

    async def before_rpc_call(self, *, rpc_message: RpcMessage, bus_client: 'lightbus.bus.BusClient'):
        pass

    async def after_rpc_call(self, *, rpc_message: RpcMessage, result_message: ResultMessage, bus_client: 'lightbus.bus.BusClient'):
        pass

    async def before_rpc_execution(self, *, rpc_message: RpcMessage, bus_client: 'lightbus.bus.BusClient'):
        pass

    async def after_rpc_execution(self, *, rpc_message: RpcMessage, result: dict, bus_client: 'lightbus.bus.BusClient'):
        pass

    async def before_event_sent(self, *, event_message: EventMessage, bus_client: 'lightbus.bus.BusClient'):
        pass

    async def before_event_execution(self, *, event_message: EventMessage, bus_client: 'lightbus.bus.BusClient'):
        pass

    async def after_event_execution(self, *, event_message: EventMessage, bus_client: 'lightbus.bus.BusClient'):
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
    """Manually set the plugins in the global plugin registry"""
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
    _plugins = OrderedDict()


def get_plugins() -> Dict[str, LightbusPlugin]:
    """Get all plugins as an ordered dictionary"""
    global _plugins
    return _plugins


def is_plugin_loaded(plugin_class: Type[LightbusPlugin]):
    global _plugins
    if not _plugins:
        return False
    return plugin_class in [type(p) for p in _plugins.values()]


async def plugin_hook(name, **kwargs):
    global _plugins
    if _plugins is None:
        raise PluginsNotLoaded("You must call load_plugins() before calling plugin_hook('{}').".format(name))
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
                await handler(**kwargs)
            )
    return return_values
