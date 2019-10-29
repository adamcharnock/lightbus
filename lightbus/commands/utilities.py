import logging

from typing import Tuple, Any

import lightbus.creation
from lightbus import configure_logging, BusPath
import lightbus.client
from lightbus.config import Config
from lightbus.exceptions import NoBusFoundInBusModule

logger = logging.getLogger(__name__)


def setup_import_parameter(argument_group):
    group = argument_group.add_mutually_exclusive_group()
    group.add_argument(
        "--bus",
        "-b",
        dest="bus_module_name",
        metavar="BUS_MODULE",
        help=(
            "The bus module to import. Example 'bus', 'my_project.bus'. Defaults to "
            "the value of the LIGHTBUS_MODULE environment variable, or 'bus'"
        ),
    )


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
