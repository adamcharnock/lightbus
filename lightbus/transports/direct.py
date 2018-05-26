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

    def __init__(self, result_transport: "DirectResultTransport"):
        self.result_transport = result_transport

    async def call_rpc(self, rpc_message: RpcMessage, options: dict):
        # Direct RPC transport calls API method immediately
        logger.debug("Directly executing RPC call for message {}".format(rpc_message))
        api = registry.get(rpc_message.api_name)
        result = await api.call(
            procedure_name=rpc_message.procedure_name, kwargs=rpc_message.kwargs
        )

        logger.debug("Sending result for message {}".format(rpc_message))
        await self.result_transport.send_result(
            rpc_message=rpc_message,
            result_message=ResultMessage(result=result),
            return_path=rpc_message.return_path,
        )
        logger.info(
            "⚡️  Directly executed RPC call & sent result for message {}.".format(rpc_message)
        )

    async def consume_rpcs(self, apis: Sequence[Api]) -> Sequence[RpcMessage]:
        raise UnsupportedUse(
            "You are using the DirectRpcTransport. This transport "
            "calls RPCs immediately & directly in the current process rather than "
            "relying on a remote process. Consuming RPCs therefore doesn't make sense "
            "in this context and is unsupported."
        )


class DirectResultTransport(ResultTransport):

    def get_return_path(self, rpc_message: RpcMessage) -> asyncio.Future:
        # We can return a future rather than a string because we know it won't have to be serialised
        return asyncio.Future()

    async def send_result(
        self, rpc_message: RpcMessage, result_message: ResultMessage, return_path: asyncio.Future
    ):
        logger.info(L("⚡️  Directly sending RPC result: {}", Bold(result_message)))
        return_path.set_result(result_message)

    async def receive_result(
        self, rpc_message: RpcMessage, return_path: asyncio.Future, options: dict
    ) -> ResultMessage:
        logger.info(L("⌛️  Awaiting result for RPC message: {}", Bold(rpc_message)))
        result = await return_path
        logger.info(L("⬅  Received result for RPC message {}: {}", rpc_message, Bold(result)))
        return result


class DirectEventTransport(EventTransport):

    def __init__(self):
        self.queue = asyncio.Queue()

    async def send_event(self, event_message: EventMessage, options: dict):
        """Publish an event"""
        logger.info(L("⚡  Directly sending event: {}", Bold(event_message)))
        await self.queue.put(event_message)

    async def fetch_events(self) -> Sequence[EventMessage]:
        """Consume RPC events for the given API"""
        logger.info(L("⌛  Awaiting all events"))
        event = await self.queue.get()
        logger.info(L("⬅  Received event {}", Bold(event)))
        yield [event]
