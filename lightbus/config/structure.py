from typing import NamedTuple, Optional, Union, Mapping, Type

from lightbus import get_available_transports
from lightbus.plugins import get_plugins

# apis:
#   default:
#     event_transport:
#       redis:
#         redis-config


def make_api_config_structure() -> NamedTuple:
    plugins = get_plugins()
    transports_by_type = get_available_transports()
    code = f"class ApiConfig(NamedTuple):\n"

    transport_config_structures = {}
    for transport_type, transports in transports_by_type.items():
        transport_config_structure = make_transport_config_structure(transport_type, transports)
        transport_config_structures[transport_config_structure.__name__] = transport_config_structure
        code += f"    {transport_type}_transport: {transport_config_structure.__name__}\n"

    code += (
        f"    rpc_timeout: int = 5\n"
        f"    event_listener_setup_timeout: int = 1\n"
        f"    event_fire_timeout: int = 1\n"
        f"    log_level: Optional[str] = None\n"
    )

    globals_ = globals().copy()
    globals_.update(transport_config_structures)
    exec(code, globals_)
    return globals_['ApiConfig']


def make_transport_config_structure(type_, transports):
    class_name = f"{type_.title()}Transport"
    code = f"class {class_name}(NamedTuple):\n"
    config_classes = {}
    for _, transport_name, transport_class in transports:
        transport_config_structure = transport_class.get_config_structure()
        if transport_config_structure:
            config_classes[transport_config_structure.__name__] = transport_config_structure
            code += f"    {transport_name}: {transport_config_structure.__name__}\n"

    globals_ = globals().copy()
    globals_.update(config_classes)
    exec(code, globals_)
    return globals_[class_name]


ApiConfig = make_api_config_structure()


class BusConfig(NamedTuple):
    schema_load_timeout: int = 5
    schema_add_api_timeout: int = 1
    schema_human_readable: bool = True
    log_level: str = 'debug'


class RootConfig(NamedTuple):
    bus: BusConfig
    apis: Mapping[str, ApiConfig]
