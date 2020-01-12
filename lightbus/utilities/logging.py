import logging
from logging import Logger

from typing import TYPE_CHECKING

from lightbus.config import Config
from lightbus.log import LightbusFormatter, LBullets, L, Bold
from lightbus.plugins import PluginRegistry
from lightbus.transports.registry import TransportRegistry

if TYPE_CHECKING:
    # pylint: disable=unused-import,cyclic-import
    from lightbus import RpcTransport, ResultTransport, EventTransport, SchemaTransport, Schema

handler = logging.StreamHandler()


def configure_logging(log_level=logging.INFO):
    logger = logging.getLogger("lightbus")
    logger.propagate = False

    formatter = LightbusFormatter()
    handler.setFormatter(formatter)

    logger.removeHandler(handler)
    logger.addHandler(handler)
    logger.setLevel(log_level)


def log_welcome_message(
    logger: Logger,
    transport_registry: TransportRegistry,
    schema: "Schema",
    plugin_registry: PluginRegistry,
    config: Config,
):
    """Show the worker-startup welcome message
    """
    logger.info(
        LBullets(
            "Lightbus is setting up",
            items={
                "service_name (set with -s or LIGHTBUS_SERVICE_NAME)": Bold(config.service_name),
                "process_name (with with -p or LIGHTBUS_PROCESS_NAME)": Bold(config.process_name),
            },
        )
    )

    # Log the transport information
    rpc_transport = transport_registry.get_rpc_transport("default", default=None)
    result_transport = transport_registry.get_result_transport("default", default=None)
    event_transport = transport_registry.get_event_transport("default", default=None)
    log_transport_information(
        rpc_transport, result_transport, event_transport, schema.schema_transport, logger
    )

    if plugin_registry._plugins:
        logger.info(
            LBullets(
                "Loaded the following plugins ({})".format(len(plugin_registry._plugins)),
                items=plugin_registry._plugins,
            )
        )
    else:
        logger.info("No plugins loaded")


def log_transport_information(
    rpc_transport: "RpcTransport",
    result_transport: "ResultTransport",
    event_transport: "EventTransport",
    schema_transport: "SchemaTransport",
    logger: Logger,
):
    def stringify_transport(transport):
        if not transport:
            return "None"

        # Rely on the transport to display itself sensibly as a string
        return L("{} @ {}", transport.__class__.__name__, Bold(transport))

    logger.info(
        LBullets(
            "Default transports are setup as follows:",
            items={
                "RPC transport": stringify_transport(rpc_transport),
                "Result transport": stringify_transport(result_transport),
                "Event transport": stringify_transport(event_transport),
                "Schema transport": stringify_transport(schema_transport),
            },
        )
    )
