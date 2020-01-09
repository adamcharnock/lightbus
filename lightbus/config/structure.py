""" Configuration structure generation

We do some pretty exciting things here to generate the configuration
structure.

We load in all available plugins and transports and get the desired
configuration format for each (via the parameters of `from_config()` in both
cases). From there we dynamically generate named tuples which form
the structure for the configuration.

Benefits of this are:

    1. Many configuration errors can be caught early
    2. We can use the schema-related code to generate a JSON schema for the config.
       This can be used for validation, auto completion, and improving tooling in general
       (smart config editing in the management UI?)
    3. Plugins and transports can be assured of reasonable sane data

See Also: https://lightbus.org/reference/configuration/
"""
import logging
import os
import socket
from enum import Enum
from typing import NamedTuple, Optional, Union, Dict

from lightbus.plugins import find_plugins
from lightbus.transports.registry import get_available_transports
from lightbus.utilities.config import random_name
from lightbus.utilities.human import generate_human_friendly_name

logger = logging.getLogger(__name__)

# pylint: disable=exec-used


def make_transport_selector_structure(type_) -> NamedTuple:
    class_name = f"{type_.title()}TransportSelector"
    code = f"class {class_name}(NamedTuple):\n    pass\n"
    variables = {}
    transports = get_available_transports(type_)
    for transport_name, transport_class in transports.items():
        variables[transport_class.__name__] = transport_class
        code += f"    {transport_name}: Optional[{transport_class.__name__}.Config] = None\n"

    globals_ = globals().copy()
    globals_.update(variables)
    exec(code, globals_)  # nosec
    return globals_[class_name]


def make_plugin_selector_structure() -> NamedTuple:
    class_name = f"PluginSelector"
    code = f"class {class_name}(NamedTuple):\n    pass\n"
    variables = {}

    for plugin_name, plugin_class in find_plugins().items():
        plugin_class_name = plugin_class.__name__
        variables[plugin_class_name] = plugin_class
        code += f"    {plugin_name}: Optional[{plugin_class_name}.Config] = {plugin_class_name}.Config()\n"

    globals_ = globals().copy()
    globals_.update(variables)
    exec(code, globals_)  # nosec
    return globals_[class_name]


RpcTransportSelector = make_transport_selector_structure("rpc")
ResultTransportSelector = make_transport_selector_structure("result")
EventTransportSelector = make_transport_selector_structure("event")
SchemaTransportSelector = make_transport_selector_structure("schema")

PluginSelector = make_plugin_selector_structure()


class LogLevelEnum(Enum):
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ApiValidationConfig(NamedTuple):
    outgoing: bool = True
    incoming: bool = True


class ApiConfig:
    rpc_timeout: int = 5
    event_listener_setup_timeout: int = 1
    event_fire_timeout: int = 5
    validate: Optional[Union[ApiValidationConfig, bool]] = ApiValidationConfig()
    event_transport: EventTransportSelector = None
    rpc_transport: RpcTransportSelector = None
    result_transport: ResultTransportSelector = None
    strict_validation: bool = False
    #: Cast values before calling event listeners and RPCs
    cast_values: bool = True

    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)

        self._normalise_validate()

    @classmethod
    def __from_bus__(cls, data: dict):
        data = {k: v for k, v in data.items() if k in dir(cls)}
        return cls(**data)

    def _normalise_validate(self):
        if self.validate in (True, False):
            # Expand out the true/false shortcut
            self.validate = ApiValidationConfig(outgoing=self.validate, incoming=self.validate)
        elif isinstance(self.validate, dict):
            # pylint: disable=not-a-mapping
            self.validate = ApiValidationConfig(**self.validate)


class SchemaConfig(NamedTuple):
    human_readable: bool = True
    ttl: int = 60
    transport: SchemaTransportSelector = None


class BusConfig(NamedTuple):
    log_level: LogLevelEnum = LogLevelEnum.INFO
    schema: SchemaConfig = SchemaConfig()


class RootConfig:
    service_name: str = "{friendly}"
    process_name: str = "{random4}"
    bus: BusConfig = BusConfig()
    apis: Dict[str, ApiConfig] = {}
    plugins: PluginSelector = PluginSelector()

    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)

        self.set_service_name(self.service_name)
        self.set_process_name(self.process_name)

    @classmethod
    def __from_bus__(cls, data: dict):
        data = {k: v for k, v in data.items() if k in dir(cls)}
        return cls(**data)

    def set_service_name(self, name):
        self.service_name = self._format_name(name)

    def set_process_name(self, name):
        self.process_name = self._format_name(name)

    def _format_name(self, name):
        random_string = random_name(length=16)
        return name.format(
            hostname=socket.gethostname().lower(),
            pid=os.getpid(),
            random4=random_string[:4],
            random8=random_string[:8],
            random16=random_string[:16],
            friendly=generate_human_friendly_name(),
        )
