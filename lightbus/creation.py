"""Utility functions relating to bus creation"""
import logging
import os
import sys
import weakref
from typing import Union, Mapping, Type, List

from lightbus import Schema
from lightbus.api import ApiRegistry
from lightbus.client.docks.event import EventDock
from lightbus.client.docks.rpc_result import RpcResultDock
from lightbus.client.subclients.event import EventClient
from lightbus.client.subclients.rpc_result import RpcResultClient
from lightbus.client.utilities import ErrorQueueType
from lightbus.hooks import HookRegistry
from lightbus.internal_apis import LightbusStateApi, LightbusMetricsApi
from lightbus.plugins import PluginRegistry
from lightbus.utilities.async_tools import get_event_loop
from lightbus.utilities.internal_queue import InternalQueue
from lightbus.utilities.features import ALL_FEATURES, Feature
from lightbus.config.structure import RootConfig
from lightbus.log import Bold
from lightbus.path import BusPath
from lightbus.client.bus_client import BusClient
from lightbus.config import Config
from lightbus.exceptions import FailedToImportBusModule
from lightbus.transports.registry import TransportRegistry
from lightbus.utilities.importing import import_module_from_string
from lightbus.utilities.logging import log_welcome_message

__all__ = ["create", "load_config", "import_bus_module", "get_bus"]
logger = logging.getLogger(__name__)


def create(
    config: Union[dict, RootConfig] = None,
    *,
    config_file: str = None,
    service_name: str = None,
    process_name: str = None,
    features: List[Union[Feature, str]] = ALL_FEATURES,
    client_class: Type[BusClient] = BusClient,
    node_class: Type[BusPath] = BusPath,
    plugins=None,
    flask: bool = False,
    **kwargs,
) -> BusPath:
    """
    Create a new bus instance which can be used to access the bus.

    Typically this will be used as follows:

        import lightbus

        bus = lightbus.create()

    This will be a `BusPath` instance. If you wish to access the lower
    level `BusClient` you can do so via `bus.client`.

    Args:
        config (dict, Config): The config object or dictionary to load
        config_file (str): The path to a config file to load (should end in .json or .yaml)
        service_name (str): The name of this service - will be used when creating event consumer groups
        process_name (str): The unique name of this process - used when retrieving unprocessed events following a crash
        client_class (Type[BusClient]): The class from which the bus client will be instantiated
        node_class (BusPath): The class from which the bus path will be instantiated
        plugins (list): A list of plugin instances to load
        flask (bool): Are we using flask? If so we will make sure we don't start lightbus in the reloader process
        **kwargs (): Any additional instantiation arguments to be passed to `client_class`.

    Returns: BusPath

    """
    if flask:
        in_flask_server = sys.argv[0].endswith("flask") and "run" in sys.argv
        if in_flask_server and os.environ.get("WERKZEUG_RUN_MAIN", "").lower() != "true":
            # Flask has a reloader process that shouldn't start a lightbus client
            return

    # Ensure an event loop exists, as creating InternalQueue
    # objects requires that we have one.
    get_event_loop()

    # If were are running via the Lightbus CLI then we may have
    # some command line arguments we need to apply.
    # pylint: disable=cyclic-import,import-outside-toplevel
    from lightbus.commands import COMMAND_PARSED_ARGS

    config_file = COMMAND_PARSED_ARGS.get("config_file", None) or config_file
    service_name = COMMAND_PARSED_ARGS.get("service_name", None) or service_name
    process_name = COMMAND_PARSED_ARGS.get("process_name", None) or process_name

    if config is None:
        config = load_config(
            from_file=config_file, service_name=service_name, process_name=process_name
        )

    if isinstance(config, Mapping):
        config = Config.load_dict(config or {})
    elif isinstance(config, RootConfig):
        config = Config(config)

    transport_registry = kwargs.pop("transport_registry", None) or TransportRegistry().load_config(
        config
    )

    schema = Schema(
        schema_transport=transport_registry.get_schema_transport(),
        max_age_seconds=config.bus().schema.ttl,
        human_readable=config.bus().schema.human_readable,
    )

    error_queue: ErrorQueueType = InternalQueue()

    # Plugin registry

    plugin_registry = PluginRegistry()
    if plugins is None:
        logger.debug("Auto-loading any installed Lightbus plugins...")
        plugin_registry.autoload_plugins(config)
    else:
        logger.debug("Loading explicitly specified Lightbus plugins....")
        plugin_registry.set_plugins(plugins)

    # Hook registry

    hook_registry = HookRegistry(
        error_queue=error_queue, execute_plugin_hooks=plugin_registry.execute_hook
    )

    # API registry

    api_registry = ApiRegistry()
    api_registry.add(LightbusStateApi())
    api_registry.add(LightbusMetricsApi())

    events_queue_client_to_dock = InternalQueue()
    events_queue_dock_to_client = InternalQueue()

    event_client = EventClient(
        api_registry=api_registry,
        hook_registry=hook_registry,
        config=config,
        schema=schema,
        error_queue=error_queue,
        consume_from=events_queue_dock_to_client,
        produce_to=events_queue_client_to_dock,
    )

    event_dock = EventDock(
        transport_registry=transport_registry,
        api_registry=api_registry,
        config=config,
        error_queue=error_queue,
        consume_from=events_queue_client_to_dock,
        produce_to=events_queue_dock_to_client,
    )

    rpcs_queue_client_to_dock = InternalQueue()
    rpcs_queue_dock_to_client = InternalQueue()

    rpc_result_client = RpcResultClient(
        api_registry=api_registry,
        hook_registry=hook_registry,
        config=config,
        schema=schema,
        error_queue=error_queue,
        consume_from=rpcs_queue_dock_to_client,
        produce_to=rpcs_queue_client_to_dock,
    )

    rpc_result_dock = RpcResultDock(
        transport_registry=transport_registry,
        api_registry=api_registry,
        config=config,
        error_queue=error_queue,
        consume_from=rpcs_queue_client_to_dock,
        produce_to=rpcs_queue_dock_to_client,
    )

    client = client_class(
        config=config,
        hook_registry=hook_registry,
        plugin_registry=plugin_registry,
        features=features,
        schema=schema,
        api_registry=api_registry,
        event_client=event_client,
        rpc_result_client=rpc_result_client,
        error_queue=error_queue,
        transport_registry=transport_registry,
        **kwargs,
    )

    # Pass the client to any hooks
    # (use a weakref to prevent circular references)
    hook_registry.set_extra_parameter("client", weakref.proxy(client))

    # We don't do this normally as the docks do not need to be
    # accessed directly, but this is useful in testing
    # TODO: Testing flag removed, but these are only needed in testing.
    #       Perhaps wrap them up in a way that makes this obvious
    client.event_dock = event_dock
    client.rpc_result_dock = rpc_result_dock

    log_welcome_message(
        logger=logger,
        transport_registry=transport_registry,
        schema=schema,
        plugin_registry=plugin_registry,
        config=config,
    )

    return node_class(name="", parent=None, client=client)


def load_config(
    from_file: str = None, service_name: str = None, process_name: str = None, quiet: bool = False
) -> Config:
    from_file = from_file or os.environ.get("LIGHTBUS_CONFIG")

    if from_file:
        if not quiet:
            logger.info(f"Loading config from {from_file}")
        config = Config.load_file(file_path=from_file)
    else:
        if not quiet:
            logger.info(f"No config file specified, will use default config")
        config = Config.load_dict({})

    if service_name:
        config._config.set_service_name(service_name)

    if process_name:
        config._config.set_process_name(process_name)

    return config


def import_bus_module(bus_module_name: str = None):
    """Get the bus module, importing if necessary

    The bus module is determined as follows:

      1. The bus_module_name, if present
      2. The `LIGHTBUS_MODULE` environment variable, if present
      3. Otherwise, the default of `bus` will be used
    """
    bus_module_name = bus_module_name or os.environ.get("LIGHTBUS_MODULE", "") or "bus"
    logger.info(f"Importing bus.py from {Bold(bus_module_name)}")
    try:
        bus_module = import_module_from_string(bus_module_name)
    except ImportError as e:
        raise FailedToImportBusModule(
            f"Failed to import bus module '{bus_module_name}'.\n\n"
            f"    Perhaps you need to set the LIGHTBUS_MODULE environment variable? Alternatively "
            f"you may need to configure your PYTHONPATH.\n\n"
            f"    Error was: {e}"
        )

    if not hasattr(bus_module, "bus"):
        raise FailedToImportBusModule(
            f"Bus module at '{bus_module_name}' contains no attribute named 'bus'.\n\n"
            f"    Your bus module must set a variable named bus using:\n"
            f"        bus = lightbus.create()"
        )

    if not isinstance(bus_module.bus, BusPath):
        raise FailedToImportBusModule(
            f"Bus module at '{bus_module_name}' contains an invalid value for the 'bus' attribute.\n\n"
            f"    We expected a BusPath instance, but found '{type(bus_module.bus).__name__}'.\n"
            f"    Your bus module must contain a variable named 'bus' using:\n"
            f"        bus = lightbus.create()"
        )

    return bus_module


def get_bus(bus_module_name: str = None) -> BusPath:
    """Get the bus, importing if necessary
    """
    return import_bus_module(bus_module_name).bus
