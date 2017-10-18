import asyncio
import logging
from typing import Sequence

from lightbus.transports.base import ResultTransport, RpcTransport, EventTransport
from lightbus.api import registry, Api
from lightbus.exceptions import UnsupportedUse
from lightbus.log import L, Bold
from lightbus.message import RpcMessage, ResultMessage, EventMessage


logger = logging.getLogger(__name__)


class DirectRpcTransport(RpcTransport):

    def __init__(self, result_transport: 'DirectResultTransport'):
        self.result_transport = result_transport

    async def call_rpc(self, rpc_message: RpcMessage):
        # Direct RPC transport calls API method immediately
        logger.debug("Directly executing RPC call for message {}".format(rpc_message))
        api = registry.get(rpc_message.api_name)
        result = await api.call(
            procedure_name=rpc_message.procedure_name,
            kwargs=rpc_message.kwargs
        )

        logger.debug("Sending result for message {}".format(rpc_message))
        await self.result_transport.send_result(
            rpc_message=rpc_message,
            result_message=ResultMessage(result=result)
        )
        logger.info("⚡️  Directly executed RPC call & sent result for message {}.".format(rpc_message))

    async def consume_rpcs(self, apis: Sequence[Api]) -> Sequence[RpcMessage]:
        raise UnsupportedUse(
            "You are using the DirectRpcTransport. This transport "
            "calls RPCs immediately & directly in the current process rather than "
            "relying on a remote process. Consuming RPCs therefore doesn't make sense "
            "in this context and is unsupported."
        )


class DirectResultTransport(ResultTransport):

    def get_return_path(self, rpc_message: RpcMessage) -> str:
        logger.debug("Attaching future to RPC message")
        rpc_message._direct_result_transport_future = asyncio.Future()
        return ''

    async def send_result(self, rpc_message: RpcMessage, result_message: ResultMessage):
        logger.info(L("⚡️  Directly sending RPC result: {}", Bold(result_message)))
        rpc_message._direct_result_transport_future.set_result(result_message)

    async def receive_result(self, rpc_message: RpcMessage) -> ResultMessage:
        logger.info(L("⌛️  Awaiting result for RPC message: {}", Bold(rpc_message)))
        result = await rpc_message._direct_result_transport_future
        logger.info(L("⬅  Received result for RPC message {}: {}", rpc_message, Bold(result)))
        return result


class DirectEventTransport(EventTransport):
    async def send_event(self, api, name, kwargs):
        """Publish an event"""
        raise NotImplementedError()

    async def consume_events(self) -> Sequence[EventMessage]:
        """Consume RPC events for the given API"""
        raise NotImplementedError()
