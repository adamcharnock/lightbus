import logging
from logging import Logger

from typing import TYPE_CHECKING

from lightbus.log import LightbusFormatter, LBullets, L, Bold

if TYPE_CHECKING:
    # pylint: disable=unused-import,cyclic-import
    from lightbus import RpcTransport, ResultTransport, EventTransport, SchemaTransport

handler = logging.StreamHandler()


def configure_logging(log_level=logging.INFO):
    logger = logging.getLogger("lightbus")
    logger.propagate = False

    formatter = LightbusFormatter()
    handler.setFormatter(formatter)

    logger.removeHandler(handler)
    logger.addHandler(handler)
    logger.setLevel(log_level)


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
