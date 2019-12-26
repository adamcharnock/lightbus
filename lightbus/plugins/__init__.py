import asyncio
import logging
from argparse import ArgumentParser, _ArgumentGroup, Namespace
from typing import Dict, Type, TypeVar, NamedTuple, TYPE_CHECKING

from collections import OrderedDict

from lightbus.schema.schema import Parameter
from lightbus.exceptions import PluginHookNotFound, LightbusShutdownInProgress
from lightbus.message import RpcMessage, EventMessage, ResultMessage
from lightbus.utilities.config import make_from_config_structure
from lightbus.utilities.importing import load_entrypoint_classes

if TYPE_CHECKING:
    # pylint: disable=unused-import,cyclic-import
    from lightbus.config import Config
    from lightbus.client import BusClient

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
                extra_parameters=[Parameter("enabled", bool, default=False)],
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

    async def before_worker_start(self, *, client: "BusClient"):
        """Called after the worker process is setup, but before any RPC or Event consumption"""
        pass

    async def after_worker_stopped(self, *, client: "BusClient"):
        """Called during worker process shutdown (after all RPC and Event consumption has been stopped)"""
        pass

    async def before_rpc_call(self, *, rpc_message: RpcMessage, client: "BusClient"):
        """Called before `rpc_message` is sent to the bus

        `rpc_message` is mutable and may therefore be changed prior to being sent
        """
        pass

    async def after_rpc_call(
        self, *, rpc_message: RpcMessage, result_message: ResultMessage, client: "BusClient"
    ):
        """Called after an RPC response has been received back from the bus

        `result_message` is mutable and may therefore be changed prior to it being
        passed back to the calling code
        """
        pass

    async def before_rpc_execution(self, *, rpc_message: RpcMessage, client: "BusClient"):
        """Called once an incoming RPC call is received, but before it is executed

        `rpc_message` is mutable and may therefore be changed prior to being passed to the
        handling code.
        """
        pass

    async def after_rpc_execution(
        self, *, rpc_message: RpcMessage, result_message: ResultMessage, client: "BusClient"
    ):
        """Called after an incoming RPC call has been received and executed

        `result_message` is mutable and may therefore be changed prior to being
        sent on the bus back to the caller.
        """

    async def before_event_sent(self, *, event_message: EventMessage, client: "BusClient"):
        """Called before an event to sent onto the bus

        `event_message` is mutable and may therefore be changed prior to being sent
        """
        pass

    async def after_event_sent(self, *, event_message: EventMessage, client: "BusClient"):
        """Called after an event has been sent onto the bus"""
        pass

    async def before_event_execution(self, *, event_message: EventMessage, client: "BusClient"):
        """Called once an incoming event call is received, but before its handler is executed

        `event_message` is mutable and may therefore be changed prior to being passed to the
        handling code.
        """
        pass

    async def after_event_execution(self, *, event_message: EventMessage, client: "BusClient"):
        """Called after an incoming event call has been received and its handler executed"""

    async def exception(self, *, e: Exception):
        """Called during the handling of Exception e

        This hook will only be called when running within a lightbus worker (ie. via `lightbus run`).
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
