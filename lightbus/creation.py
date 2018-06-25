import asyncio
import os
from typing import Union, Optional, Mapping

from lightbus import BusClient
from lightbus.log import LBullets, L, Bold
from lightbus.path import BusPath
from lightbus.client import logger
from lightbus.config import Config
from lightbus.exceptions import FailedToImportBusModule
from lightbus.transports.base import TransportRegistry
from lightbus.utilities.async import get_event_loop, block
from lightbus.utilities.importing import import_module_from_string

if False:
    from lightbus.transports import *


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
    loop: asyncio.AbstractEventLoop = None,
    flask: bool = False,
    **kwargs,
) -> BusPath:

    if flask and os.environ.get("WERKZEUG_RUN_MAIN", "").lower() != "true":
        # Flask has a reloader process that shouldn't start a lightbus client
        return

    from lightbus.config import Config

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

    client = client_class(transport_registry=transport_registry, config=config, loop=loop, **kwargs)
    await client.setup_async(plugins=plugins)

    return node_class(name="", parent=None, client=client)


def create(*args, **kwargs):
    loop = kwargs.get("loop") or get_event_loop()
    return block(create_async(*args, **kwargs), loop=loop, timeout=5)


def load_config(
    from_file: str = None, service_name: str = None, process_name: str = None
) -> Config:
    from_file = from_file or os.environ.get("LIGHTBUS_CONFIG")
    service_name = service_name or os.environ.get("LIGHTBUS_SERVICE_NAME")
    process_name = process_name or os.environ.get("LIGHTBUS_PROCESS_NAME")

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


def import_bus_py(bus_module_name: str = None):
    bus_module_name = bus_module_name or os.environ.get("LIGHTBUS_MODULE", "bus")
    logger.info(f"Importing bus.py from {Bold(bus_module_name)}")
    try:
        return import_module_from_string(bus_module_name)
    except ImportError as e:
        raise FailedToImportBusModule(
            f"Failed to import bus module at '{bus_module_name}'. Perhaps you "
            f"need to set the LIGHTBUS_MODULE environment variable? Alternatively "
            f"you may need to configure your PYTHONPATH. Error was: {e}"
        )
