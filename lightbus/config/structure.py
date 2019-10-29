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
import logging
import os
import socket
from enum import Enum
from typing import NamedTuple, Optional, Union, Dict, Tuple, List

from lightbus.plugins import find_plugins
from lightbus.transports.base import get_available_transports
from lightbus.utilities.config import random_name, enable_config_inheritance
from lightbus.utilities.human import generate_human_friendly_name
from lightbus.utilities.type_checks import parse_hint

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
    exec(code, globals_)  # nosec
    structure = globals_[class_name]
    return enable_config_inheritance(structure)


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


class OnError(Enum):
    IGNORE = "ignore"
    STOP_LISTENER = "stop_listener"
    SHUTDOWN = "shutdown"


@enable_config_inheritance
class ApiValidationConfig(NamedTuple):
    outgoing: bool = True
    incoming: bool = True


@enable_config_inheritance
class ApiConfig(object):
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
    on_error: OnError = OnError.SHUTDOWN

    def __init__(self, **kw):
        self._explicitly_set_fields = set(kw.keys())
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
            self.validate = ApiValidationConfig(**self.validate)


@enable_config_inheritance
class SchemaConfig(NamedTuple):
    human_readable: bool = True
    ttl: int = 60
    transport: SchemaTransportSelector = None


@enable_config_inheritance
class BusConfig(NamedTuple):
    log_level: LogLevelEnum = LogLevelEnum.INFO
    schema: SchemaConfig = SchemaConfig()


@enable_config_inheritance
class RootConfig(object):
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


class ConfigProxy(object):
    """ Provides config inheritance

    Must be instantiated with pairs of config objects and the
    dictionaries from which the object originated. For example:

        >>> proxy = ConfigProxy(
        ...     (ApiConfig(rpc_timeout=60), {"rpc_timeout": 60}),
        ...     (ApiConfig(rpc_timeout=2), {"rpc_timeout": 2})
        ... )
        >>> proxy.rpc_timeout
        60

    The dictionaries will be used to determine which keys are
    available on an object. The dictionary values are never returned.

    If a key is found in the pair's dictionary, then the corresponding
    attribute from the config object will be returned. Otherwise the next
    pair will be checked.

    If no value is found at the end of this process then we assume that
    not value has been explicitly provided. In which case the default
    determined and returned.

    The default value is determined by taking the config object of the final
    pair and retrieving whatever value the desired attribute has, regardless
    of its presence in the pair's dictionary.

    This proxy operates recursively on child config objects.
    """

    def __init__(self, *pairs: Tuple[object, dict], parents: List[str] = None):
        self.__dict__["_pairs"] = pairs
        self.__dict__["_parents"] = parents or []

    def __repr__(self):
        fallback_object = self._pairs[-1][0]
        return f"{repr(fallback_object)} (via ConfigProxy)"

    def __getattr__(self, key):
        for obj, source_data in self._pairs:
            if isinstance(source_data, dict) and key not in source_data:
                # Either:
                #   1. Key not present in the source data, so fallback to next config
                #   2. Dict does not match object structure (e.g. ApiConfig.validate)
                continue
            else:
                value = self._get_value(key)
                child_pairs = self._get_child_pairs(key)
                if self._should_proxy(obj, key):
                    return ConfigProxy(*child_pairs, parents=self._parents + [key])
                else:
                    return value

        fallback_object = self._pairs[-1][0]
        if hasattr(fallback_object, key):
            # Not found, but does exist on the object, so just return whatever
            # default value it is given
            return getattr(fallback_object, key)
        else:
            # Was not found, so raise AttributeError
            proxied_type = type(fallback_object)
            raise AttributeError(f"{proxied_type.__name__} has no attribute {repr(key)}")

    def __setattr__(self, key, value):
        raise AttributeError(
            "Setting config options at runtime is not supported. If you really must "
            "do this then modify the configuration dictionary, then pass it to "
            "Config.load_dict(...)"
        )

    def _get_child_pairs(self, key):
        child_pairs = []
        for obj, source_data in self._pairs:
            child_obj = getattr(obj, key, None)
            if isinstance(source_data, dict):
                child_dict = source_data.get(key, {})
                child_pairs.append((child_obj, child_dict))
        return child_pairs

    def _get_value(self, key):
        for obj, source_data in self._pairs:
            if isinstance(source_data, dict) and key in source_data:
                return getattr(obj, key)

        # We only end up here when the dictionary structure
        # and config structure do not match. For example,
        # this can be the case with ApiConfig.validate.
        for obj, source_data in self._pairs:
            if not isinstance(source_data, dict) and hasattr(obj, key):
                return getattr(obj, key)

        assert False, "Shouldn't happen"

    def _should_proxy(self, obj, key):
        annotations = getattr(obj, "__annotations__", {})
        key_hint = annotations.get(key)
        # Attribute set by the @enable_config_inheritance() decorator
        key_hint, child_types = parse_hint(key_hint)

        if child_types:
            types_to_check = child_types
        else:
            types_to_check = [key_hint]

        return any(getattr(t, "_enable_config_inheritance", False) for t in types_to_check)
