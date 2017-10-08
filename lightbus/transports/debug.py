import asyncio
import logging
from typing import Sequence

from lightbus.transports.base import ResultTransport, RpcTransport
from lightbus.message import RpcMessage, EventMessage, ResultMessage


logger = logging.getLogger(__name__)


class DebugRpcTransport(RpcTransport):

    async def call_rpc(self, rpc_message: RpcMessage):
        """Publish a call to a remote procedure"""
        logger.debug("Faking dispatch of message {}".format(rpc_message))

    async def consume_rpcs(self, api) -> Sequence[RpcMessage]:
        """Consume RPC calls for the given API"""
        logger.debug("Faking consumption of RPCs. Waiting 1 second before issuing fake RPC call...")
        await asyncio.sleep(1)
        logger.debug("Issuing fake RPC call")
        return [RpcMessage(api_name='my_company.auth', procedure_name='check_password', kwargs=dict(
            username='admin',
            password='secret',
        ))]

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
