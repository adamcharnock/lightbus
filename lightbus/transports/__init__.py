import logging

from lightbus.exceptions import TransportNotFound
from lightbus.utilities.importing import load_entrypoint_classes
from .base import RpcTransport, ResultTransport, EventTransport, SchemaTransport
from .debug import DebugRpcTransport, DebugResultTransport, DebugEventTransport, DebugSchemaTransport
from .direct import DirectRpcTransport, DirectResultTransport, DirectEventTransport
from .redis import RedisRpcTransport, RedisResultTransport, RedisEventTransport, RedisSchemaTransport


def get_available_transports(type_):
    loaded = load_entrypoint_classes(f'lightbus_{type_}_transports')

    return {
        name: class_
        for module_name, name, class_
        in loaded
    }

def get_transport(type_, name):
    for name_, class_ in get_available_transports(type_).items():
        if name == name_:
            return class_

    raise TransportNotFound(
        "No '{}' transport found named '{}'. Check the transport is installed and "
        "has the relevant entrypoints setup in it's setup.py file. Or perhaps "
        "you have a typo in your config file."
    )
