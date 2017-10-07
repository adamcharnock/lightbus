import asyncio
import logging
from typing import Any

from lightbus.api import registry
from lightbus.exceptions import UnsupportedUse
from lightbus.log import L, Bold
from lightbus.message import RpcMessage, EventMessage, ResultMessage

__all__ = [
    'BrokerTransport', 'ResultTransport',
    'DebugBrokerTransport', 'DebugResultTransport',
    'DirectBrokerTransport', 'DirectResultTransport',
]

logger = logging.getLogger(__name__)


class BrokerTransport(object):
    # TODO: BrokerTransport is probably the wrong name, as it implies how it should be implemented

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


class DebugBrokerTransport(BrokerTransport):

    async def call_rpc(self, rpc_message: RpcMessage):
        """Publish a call to a remote procedure"""
        logger.debug("Faking dispatch of message {}".format(rpc_message))

    async def consume_rpcs(self, api) -> RpcMessage:
        """Consume RPC calls for the given API"""
        logger.debug("Faking consumption of RPCs. Waiting 1 second before issuing fake RPC call...")
        await asyncio.sleep(1)
        logger.debug("Issuing fake RPC call")
        return RpcMessage(api_name='my_company.auth', procedure_name='check_password', kwargs=dict(
            username='admin',
            password='secret',
        ))

    async def send_event(self, api, name, kwargs):
        """Publish an event"""
        pass

    async def consume_events(self, api) -> EventMessage:
        """Consume RPC events for the given API"""
        pass


class DebugResultTransport(ResultTransport):

    def get_return_path(self, rpc_message: RpcMessage) -> str:
        return 'debug://foo'

    async def send(self, rpc_message: RpcMessage, result_message: ResultMessage):
        logger.info("Faking sending of result: {}".format(result_message))

    async def receive(self, rpc_message: RpcMessage) -> ResultMessage:
        logger.info("⌛ Faking listening for results. Will issue fake result in 0.5 seconds...")
        await asyncio.sleep(0.5)
        logger.debug('Faking received result')

        return ResultMessage(result='Fake result')


class DirectBrokerTransport(BrokerTransport):

    def __init__(self, result_transport: 'DirectResultTransport'):
        self.result_transport = result_transport

    async def call_rpc(self, rpc_message: RpcMessage):
        # Direct broker calls API method immediately
        logger.debug("Directly executing RPC call for message {}".format(rpc_message))
        api = registry.get(rpc_message.api_name)
        result = await api.call(
            procedure_name=rpc_message.procedure_name,
            kwargs=rpc_message.kwargs
        )

        logger.debug("Sending result for message {}".format(rpc_message))
        await self.result_transport.send(
            rpc_message=rpc_message,
            result_message=ResultMessage(result=result)
        )
        logger.info("⚡️  Directly executed RPC call & sent result for message {}.".format(rpc_message))

    async def consume_rpcs(self, api) -> RpcMessage:
        raise UnsupportedUse(
            "You are using the DirectBrokerTransport. This transport "
            "calls RPCs immediately & directly in the current process rather than "
            "relying on a remote process. Consuming RPCs therefore doesn't make sense "
            "in this context and is unsupported."
        )

    async def send_event(self, api, name, kwargs):
        """Publish an event"""
        pass

    async def consume_events(self, api) -> EventMessage:
        """Consume RPC events for the given API"""
        pass


class DirectResultTransport(ResultTransport):

    def get_return_path(self, rpc_message: RpcMessage) -> str:
        logger.debug("Attaching future to RPC message")
        rpc_message._direct_result_transport_future = asyncio.Future()
        return ''

    async def send(self, rpc_message: RpcMessage, result_message: ResultMessage):
        logger.info(L("⚡️  Directly sending RPC result: {}", Bold(result_message)))
        rpc_message._direct_result_transport_future.set_result(result_message)

    async def receive(self, rpc_message: RpcMessage) -> ResultMessage:
        logger.info(L("⌛️  Awaiting result for RPC message: {}", Bold(rpc_message)))
        result = await rpc_message._direct_result_transport_future
        logger.info(L("✅  Received result for RPC message {}: {}", rpc_message, Bold(result)))
        return result


