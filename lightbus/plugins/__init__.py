import asyncio
import logging
import traceback
from argparse import ArgumentParser, _ArgumentGroup, Namespace
from typing import Dict, Type, TypeVar, NamedTuple

from collections import OrderedDict

import lightbus
from lightbus.schema.schema import Parameter
from lightbus.exceptions import (
    PluginsNotLoaded,
    PluginHookNotFound,
    InvalidPlugins,
    LightbusShutdownInProgress,
)
from lightbus.message import RpcMessage, EventMessage, ResultMessage
from lightbus.utilities.async_tools import run_user_provided_callable
from lightbus.utilities.config import make_from_config_structure
from lightbus.utilities.importing import load_entrypoint_classes

if False:
    # pylint: disable=unused-import
    from lightbus.config import Config

ENTRYPOINT_NAME = "lightbus_plugins"


logger = logging.getLogger(__name__)

T = TypeVar("T")


class PluginMetaclass(type):
    def __new__(mcs, name, bases, attrs, **kwds):
        cls = super().__new__(mcs, name, bases, attrs)
        if not hasattr(cls, f"{name}Config") and hasattr(cls, "from_config"):
            cls.Config = make_from_config_structure(
                class_name=name,
                from_config_method=cls.from_config,
                extra_parameters=[Parameter("enabled", bool, default=True)],
            )
        return cls


class LightbusPlugin(metaclass=PluginMetaclass):
    priority = 1000

    @classmethod
    def from_config(cls: Type[T], *, config) -> T:
        return cls()

    def __str__(self):
        return "{}.{}".format(self.__class__.__module__, self.__class__.__name__)

    async def before_parse_args(self, *, parser: ArgumentParser, subparsers: _ArgumentGroup):
        """ Setup command line argument parser

        Configuration is not available within the before_parse_args()
        hook. This hooks will be called in a separate throw-away instance of this Plugin.

        Note that we don't have an after_parse_args plugin hook. Instead we use the receive_args
        hook which is called once we have instantiated our plugins.
        """
        pass

    async def receive_args(self, args: Namespace):
        """ Received the parsed command line arguments

        This will be called once the plugins have been instantiated. You can therefore
        safely modify your plugins state at this time (unlike in before_parse_args)
        """
        pass

    async def before_server_start(self, *, client: "lightbus.client.BusClient"):
        pass

    async def after_server_stopped(self, *, client: "lightbus.client.BusClient"):
        pass

    async def before_rpc_call(
        self, *, rpc_message: RpcMessage, client: "lightbus.client.BusClient"
    ):
        pass

    async def after_rpc_call(
        self,
        *,
        rpc_message: RpcMessage,
        result_message: ResultMessage,
        client: "lightbus.client.BusClient",
    ):
        pass

    async def before_rpc_execution(
        self, *, rpc_message: RpcMessage, client: "lightbus.client.BusClient"
    ):
        pass

    async def after_rpc_execution(
        self,
        *,
        rpc_message: RpcMessage,
        result_message: ResultMessage,
        client: "lightbus.client.BusClient",
    ):
        pass

    async def before_event_sent(
        self, *, event_message: EventMessage, client: "lightbus.client.BusClient"
    ):
        pass

    async def after_event_sent(
        self, *, event_message: EventMessage, client: "lightbus.client.BusClient"
    ):
        pass

    async def before_event_execution(
        self, *, event_message: EventMessage, client: "lightbus.client.BusClient"
    ):
        pass

    async def after_event_execution(
        self, *, event_message: EventMessage, client: "lightbus.client.BusClient"
    ):
        pass

    async def exception(self, *, e: Exception):
        """Called during the handling of Exception e

        This hook will only be called when running lightbus via `lightbus run`.
        """
        pass


def instantiate_plugin(config: "Config", plugin_config: NamedTuple, cls: Type[LightbusPlugin]):
    options = plugin_config._asdict()
    options.pop("enabled")
    return cls.from_config(config=config, **options)


def find_plugins() -> Dict[str, Type[LightbusPlugin]]:
    """Discover available plugin classes using the 'lightbus_plugins' entrypoint
    """
    available_plugin_classes = load_entrypoint_classes(ENTRYPOINT_NAME)
    available_plugin_classes = sorted(available_plugin_classes, key=lambda v: v[-1].priority)

    plugins = OrderedDict()
    for module_name, name, plugin in available_plugin_classes:
        if name in plugins:
            pass
        plugins[name] = plugin

    return plugins


class PluginRegistry:
    VALID_HOOK_NAMES = {k for k in LightbusPlugin.__dict__ if not k.startswith("_")}

    def __init__(self):
        self._plugins = []

    def autoload_plugins(self, config: "Config"):
        """Autoload this registry with plugins from the 'lightbus_plugins' entrypoint"""
        for name, cls in find_plugins().items():
            plugin_config = config.plugin(name)
            if plugin_config.enabled:
                self._plugins.append(
                    instantiate_plugin(config=config, plugin_config=plugin_config, cls=cls)
                )

        return self._plugins

    def set_plugins(self, plugins: list):
        """Manually set the plugins in this registry"""
        self._plugins = plugins

    def is_plugin_loaded(self, plugin_class: Type[LightbusPlugin]):
        return plugin_class in [type(p) for p in self._plugins]

    async def execute_hook(self, name, **kwargs):
        if name not in self.VALID_HOOK_NAMES:
            raise PluginHookNotFound(
                "Plugin hook '{}' could not be found. Must be one of: {}".format(
                    name, ", ".join(self.VALID_HOOK_NAMES)
                )
            )

        return_values = []
        for plugin in self._plugins:
            handler = getattr(plugin, name, None)
            if handler:
                try:
                    return_values.append(await handler(**kwargs))
                except asyncio.CancelledError:
                    raise
                except LightbusShutdownInProgress as e:
                    logger.info("Shutdown in progress: {}".format(e))
                except Exception:
                    raise

        return return_values
