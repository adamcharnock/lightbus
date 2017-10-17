from typing import Sequence

from lightbus.api import Api
from lightbus.message import RpcMessage, EventMessage, ResultMessage


class RpcTransport(object):

    async def call_rpc(self, rpc_message: RpcMessage):
        """Publish a call to a remote procedure"""
        raise NotImplementedError()

    async def consume_rpcs(self, apis: Sequence[Api]) -> Sequence[RpcMessage]:
        """Consume RPC calls for the given API"""
        raise NotImplementedError()


class ResultTransport(object):

    def get_return_path(self, rpc_message: RpcMessage) -> str:
        raise NotImplementedError()

    async def send(self, rpc_message: RpcMessage, result_message: ResultMessage):
        """Send a result back to the caller

        Args:
            rpc_message (): The original message received from the client
            result_message (): The result message to be sent back to the client
        """
        raise NotImplementedError()

    async def receive(self, rpc_message: RpcMessage) -> ResultMessage:
        """Receive the response for the given message

        Args:
            rpc_message (): The original message sent to the server
        """
        raise NotImplementedError()


class EventTransport(object):

    def send_event(self, api, name, kwargs):
        """Publish an event"""
        raise NotImplementedError()

    async def consume_events(self, api) -> EventMessage:
        """Consume RPC events for the given API"""
        raise NotImplementedError()
