import asyncio
import logging
from typing import Sequence

from lightbus.transports.base import ResultTransport, RpcTransport, EventTransport
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


class DebugResultTransport(ResultTransport):

    def get_return_path(self, rpc_message: RpcMessage) -> str:
        return 'debug://foo'

    async def send_result(self, rpc_message: RpcMessage, result_message: ResultMessage):
        logger.info("Faking sending of result: {}".format(result_message))

    async def receive_result(self, rpc_message: RpcMessage) -> ResultMessage:
        logger.info("⌛ Faking listening for results. Will issue fake result in 0.5 seconds...")
        await asyncio.sleep(0.5)
        logger.debug('Faking received result')

        return ResultMessage(result='Fake result')


class DebugEventTransport(EventTransport):

    def send_event(self, event_message: EventMessage):
        """Publish an event"""
        logger.info(" Faking sending of event {}.{} with kwargs: {}".format(
            event_message.api_name,
            event_message.event_name,
            event_message.kwargs
        ))

    async def consume_events(self) -> EventMessage:
        """Consume RPC events for the given API"""
        logger.info("⌛ Faking listening for events. Will issue a fake event in 10 seconds...")
        await asyncio.sleep(10)
        logger.debug('Faking received result')

        return EventMessage(api_name='foo.bar', event_name='fake_event', kwargs={'example': 'value'})

    async def add_api(self, api_name):
        pass

    async def remove_api(self, api_name):
        pass
