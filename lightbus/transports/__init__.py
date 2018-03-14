import logging

from lightbus.exceptions import TransportNotFound
from lightbus.utilities.importing import load_entrypoint_classes
from .base import RpcTransport, ResultTransport, EventTransport, SchemaTransport
from .debug import DebugRpcTransport, DebugResultTransport, DebugEventTransport, DebugSchemaTransport
from .direct import DirectRpcTransport, DirectResultTransport, DirectEventTransport
from .redis import RedisRpcTransport, RedisResultTransport, RedisEventTransport, RedisSchemaTransport


def get_available_transports():
    return {
        'event': load_entrypoint_classes('lightbus_event_transports'),
        'rpc': load_entrypoint_classes('lightbus_rpc_transports'),
        'result': load_entrypoint_classes('lightbus_result_transports'),
        'schema': load_entrypoint_classes('lightbus_schema_transports'),
    }


def get_transport(type_, name):
    for _, name_, class_ in get_available_transports()[type_]:
        if name == name_:
            return class_

    raise TransportNotFound(
        "No '{}' transport found named '{}'. Check the transport is installed and "
        "has the relevant entrypoints setup in it's setup.py file. Or perhaps "
        "you have a typo in your config file."
    )
