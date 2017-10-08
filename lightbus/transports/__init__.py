import logging

from lightbus.message import RpcMessage, EventMessage, ResultMessage

from .debug import DebugRpcTransport, DebugResultTransport
from .direct import DirectRpcTransport, DirectResultTransport

__all__ = [
    'RpcTransport', 'ResultTransport',
]

logger = logging.getLogger(__name__)


class RpcTransport(object):

    async def call_rpc(self, rpc_message: RpcMessage):
        """Publish a call to a remote procedure"""
        pass

    async def consume_rpcs(self, api) -> RpcMessage:
        """Consume RPC calls for the given API"""
        pass

    async def send_event(self, api, name, kwargs):
        """Publish an event"""
        pass

    async def consume_events(self, api) -> EventMessage:
        """Consume RPC events for the given API"""
        pass


class ResultTransport(object):

    def get_return_path(self, rpc_message: RpcMessage) -> str:
        return ''

    async def send(self, rpc_message: RpcMessage, result_message: ResultMessage):
        """Send a result back to the caller

        Args:
            rpc_message (): The original message received from the client
            result_message (): The result message to be sent back to the client
        """
        pass

    async def receive(self, rpc_message: RpcMessage) -> ResultMessage:
        """Receive the response for the given message

        Args:
            rpc_message (): The original message sent to the server
        """
        pass


