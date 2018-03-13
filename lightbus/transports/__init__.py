import logging

from lightbus.utilities import load_entrypoint_classes
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
