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

"""
import inspect
import logging
from typing import NamedTuple, Optional, Union, Mapping, Type, Dict

from enum import Enum

from lightbus.transports.base import get_available_transports
from lightbus.plugins import get_plugins, find_plugins

logger = logging.getLogger(__name__)


def make_transport_selector_structure(type_) -> NamedTuple:
    class_name = f"{type_.title()}TransportSelector"
    code = f"class {class_name}(NamedTuple):\n    pass\n"
    vars = {}
    transports = get_available_transports(type_)
    for transport_name, transport_class in transports.items():
        vars[transport_class.__name__] = transport_class
        code += f"    {transport_name}: Optional[{transport_class.__name__}.Config] = None\n"

    globals_ = globals().copy()
    globals_.update(vars)
    exec(code, globals_)
    return globals_[class_name]


def make_plugin_selector_structure() -> NamedTuple:
    class_name = f"PluginSelector"
    code = f"class {class_name}(NamedTuple):\n    pass\n"
    vars = {}

    for plugin_name, plugin_class in find_plugins().items():
        plugin_class_name = plugin_class.__name__
        vars[plugin_class_name] = plugin_class
        code += f"    {plugin_name}: Optional[{plugin_class_name}.Config] = {plugin_class_name}.Config()\n"

    globals_ = globals().copy()
    globals_.update(vars)
    exec(code, globals_)
    return globals_[class_name]


# TODO: Rename to specifiers?
RpcTransportSelector = make_transport_selector_structure('rpc')
ResultTransportSelector = make_transport_selector_structure('result')
EventTransportSelector = make_transport_selector_structure('event')
SchemaTransportSelector = make_transport_selector_structure('schema')

PluginSelector = make_plugin_selector_structure()


class LogLevelEnum(Enum):
    DEBUG = 'debug'
    INFO = 'info'
    WARNING = 'warning'
    ERROR = 'error'
    CRITICAL = 'critical'


class ApiValidationConfig(NamedTuple):
    outgoing: bool = True
    incoming: bool = True


class ApiConfig(object):
    rpc_timeout: int = 5
    event_listener_setup_timeout: int = 1
    event_fire_timeout: int = 1
    validate: Optional[Union[ApiValidationConfig, bool]] = True
    event_transport: EventTransportSelector = None
    rpc_transport: RpcTransportSelector = None
    result_transport: ResultTransportSelector = None
    schema_transport: SchemaTransportSelector = None
    strict_validation: bool = False

    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)

        self._normalise_validate()

    def _normalise_validate(self):
        if self.validate in (True, False):
            # Expand out the true/false shortcut
            self.validate = ApiValidationConfig(outgoing=self.validate, incoming=self.validate)
        else:
            self.validate = ApiValidationConfig(**self.validate)


class BusConfig(NamedTuple):
    schema_load_timeout: int = 5
    schema_add_api_timeout: int = 1
    schema_human_readable: bool = True
    schema_ttl: int = 60
    log_level: LogLevelEnum = LogLevelEnum.WARNING


class RootConfig(NamedTuple):
    bus: BusConfig = BusConfig()
    apis: Dict[str, ApiConfig] = {}
    plugins: PluginSelector = PluginSelector()
