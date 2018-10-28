"""Utility functions relating to bus creation"""

import os
import sys
from typing import Union, Optional, Mapping

from lightbus import BusClient
from lightbus.log import LBullets, L, Bold
from lightbus.path import BusPath
from lightbus.client import logger
from lightbus.config import Config
from lightbus.exceptions import FailedToImportBusModule
from lightbus.transports.base import TransportRegistry
from lightbus.utilities.async_tools import block
from lightbus.utilities.importing import import_module_from_string

if False:
    from lightbus.transports import *

__all__ = ["create", "create_async", "load_config", "import_bus_module", "get_bus"]


async def create_async(
    config: Union[dict, Config] = None,
    *,
    config_file: str = None,
    service_name: str = None,
    process_name: str = None,
    rpc_transport: Optional["RpcTransport"] = None,
    result_transport: Optional["ResultTransport"] = None,
    event_transport: Optional["EventTransport"] = None,
    schema_transport: Optional["SchemaTransport"] = None,
    client_class=BusClient,
    node_class=BusPath,
    plugins=None,
    flask: bool = False,
    **kwargs,
) -> BusPath:
    """
    Create a new bus instance which can be used to access the bus.

    Typically this will be used as follows:

        import lightbus

        bus = await lightbus.create_async()

    This will be a `BusPath` instance. If you wish to access the lower
    level `BusClient` you can do so via `bus.client`.

    See Also:

        `create()` - The synchronous wrapper for this function

    Args:
        config (dict, Config): The config object or dictionary to load
        config_file (str): The path to a config file to load (should end in .json or .yaml)
        service_name (str): The name of this service - will be used when creating event consumer groups
        process_name (str): The unique name of this process - used when retrieving unprocessed events following a crash
        rpc_transport (RpcTransport): The RPC transport instance to use, defaults to Redis
        result_transport (ResultTransport): The result transport instance to use, defaults to Redis
        event_transport (EventTransport): The event transport instance to use, defaults to Redis
        schema_transport (SchemaTransport): The schema transport instance to use, defaults to Redis
        client_class (BusClient): The class from which the bus client will be instantiated
        node_class (BusPath): The class from which the bus path will be instantiated
        plugins (list): A list of plugin instances to load
        flask (bool): Are we using flask? If so we will make sure we don't start lightbus in the reloader process
        **kwargs (): Any additional instantiation arguments to be passed to `client_class`.

    Returns:

    """
    if flask:
        in_flask_server = sys.argv[0].endswith("flask") and "run" in sys.argv
        if in_flask_server and os.environ.get("WERKZEUG_RUN_MAIN", "").lower() != "true":
            # Flask has a reloader process that shouldn't start a lightbus client
            return

    from lightbus.config import Config

    # If were are running via the Lightbus CLI then we may have
    # some command line arguments we need to apply.
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

    transport_registry = TransportRegistry().load_config(config)

    # Set transports if specified
    if rpc_transport:
        transport_registry.set_rpc_transport("default", rpc_transport)

    if result_transport:
        transport_registry.set_result_transport("default", result_transport)

    if event_transport:
        transport_registry.set_event_transport("default", event_transport)

    if schema_transport:
        transport_registry.set_schema_transport(schema_transport)

    client = client_class(transport_registry=transport_registry, config=config, **kwargs)
    await client.setup_async(plugins=plugins)

    return node_class(name="", parent=None, client=client)


def create(*args, **kwargs) -> BusPath:
    """
    Create a new bus instance which can be used to access the bus.

    Typically this will be used as follows:

        import lightbus

        bus = lightbus.create()

    See Also: This function is a wrapper around `create_async()`, see `create_async()`
    for a list of arguments

    """
    return block(create_async(*args, **kwargs), timeout=5)


def load_config(
    from_file: str = None, service_name: str = None, process_name: str = None
) -> Config:
    from_file = from_file or os.environ.get("LIGHTBUS_CONFIG")

    if from_file:
        logger.info(f"Loading config from {from_file}")
        config = Config.load_file(file_path=from_file)
    else:
        logger.info(f"No config file specified, will use default config")
        config = Config.load_dict({})

    if service_name:
        config._config.set_service_name(service_name)

    if process_name:
        config._config.set_process_name(process_name)

    return config


def import_bus_module(bus_module_name: str = None):
    bus_module_name = bus_module_name or os.environ.get("LIGHTBUS_MODULE", "bus")
    logger.info(f"Importing bus.py from {Bold(bus_module_name)}")
    try:
        bus_module = import_module_from_string(bus_module_name)
    except ImportError as e:
        raise FailedToImportBusModule(
            f"Failed to import bus module at '{bus_module_name}'. Perhaps you "
            f"need to set the LIGHTBUS_MODULE environment variable? Alternatively "
            f"you may need to configure your PYTHONPATH. Error was: {e}"
        )

    if not hasattr(bus_module, "bus"):
        raise FailedToImportBusModule(
            f"Bus module at '{bus_module_name}' contains no attribute named 'bus'.\n"
            f"Your bus module must set a variable named bus using:\n"
            f"     bus = lightbus.create()"
        )

    if not isinstance(bus_module.bus, BusPath):
        raise FailedToImportBusModule(
            f"Bus module at '{bus_module_name}' contains an invalid value for the 'bus' attribute.\n"
            f"We expected a BusPath instance, but found '{type(bus_module.bus).__name__}'.\n"
            f"Your bus module must set a variable named bus using:\n"
            f"     bus = lightbus.create()"
        )

    return bus_module


def get_bus(bus_module_name: str = None) -> BusPath:
    return import_bus_module(bus_module_name).bus
