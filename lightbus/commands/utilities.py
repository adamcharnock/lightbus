import logging

from typing import Tuple, Any

import lightbus.creation
from lightbus import configure_logging, BusPath
import lightbus.client
from lightbus.config import Config
from lightbus.exceptions import NoBusFoundInBusModule

logger = logging.getLogger(__name__)


def import_bus(args) -> Tuple[Any, BusPath]:
    bus_module = lightbus.creation.import_bus_module(args.bus_module_name)
    try:
        return bus_module, bus_module.bus
    except AttributeError:
        raise NoBusFoundInBusModule(
            f"Bus module at {bus_module.__file__} contains no variable named 'bus'. "
            f"Your bus module should contain the line 'bus = lightbus.create()'."
        )


def setup_logging(override: str, config: Config):
    configure_logging(log_level=(override or config.bus().log_level.value).upper())


def setup_common_arguments(parser):
    """Set common arguments needed by all commands"""
    general_argument_group = parser.add_argument_group(title="Common arguments")
    general_argument_group.add_argument(
        "--bus",
        "-b",
        dest="bus_module_name",
        metavar="BUS_MODULE",
        help=(
            "The bus module to import. Example 'bus', 'my_project.bus'. Defaults to "
            "the value of the LIGHTBUS_MODULE environment variable, or 'bus'"
        ),
    )
    general_argument_group.add_argument(
        "--service-name",
        "-s",
        help="Name of service in which this process resides. YOU SHOULD "
        "LIKELY SET THIS IN PRODUCTION. Can also be set using the "
        "LIGHTBUS_SERVICE_NAME environment. Will default to a random string.",
    )
    general_argument_group.add_argument(
        "--process-name",
        "-p",
        help="A unique name of this process within the service. Can also be set using the "
        "LIGHTBUS_PROCESS_NAME environment. Will default to a random string.",
    )
    general_argument_group.add_argument(
        "--config", dest="config_file", help="Config file to load, JSON or YAML", metavar="FILE"
    )
    general_argument_group.add_argument(
        "--log-level",
        help="Set the log level. Overrides any value set in config. "
        "One of debug, info, warning, critical, exception.",
        metavar="LOG_LEVEL",
    )
