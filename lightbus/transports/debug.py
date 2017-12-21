import asyncio
import logging
from typing import Sequence

import asyncio_extras

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

    async def send_result(self, rpc_message: RpcMessage, result_message: ResultMessage, return_path: str):
        logger.info("Faking sending of result: {}".format(result_message))

    async def receive_result(self, rpc_message: RpcMessage, return_path: str) -> ResultMessage:
        logger.info("⌛ Faking listening for results. Will issue fake result in 0.5 seconds...")
        await asyncio.sleep(0.5)
        logger.debug('Faking received result')

        return ResultMessage(result='Fake result')


class DebugEventTransport(EventTransport):

    def __init__(self):
        self._task = None
        self._events = set()

    async def send_event(self, event_message: EventMessage):
        """Publish an event"""
        logger.info(" Faking sending of event {}.{} with kwargs: {}".format(
            event_message.api_name,
            event_message.event_name,
            event_message.kwargs
        ))

    async def fetch_events(self) -> Sequence[EventMessage]:
        """Consume RPC events for the given API"""

        logger.info("⌛ Faking listening for events {}. Will issue a fake event in 2 seconds...".format(self._events))
        self._task = asyncio.ensure_future(asyncio.sleep(2))

        try:
            await self._task
        except asyncio.CancelledError as e:
            logger.debug('Event consumption cancelled.')
            yield []
        else:
            logger.debug('Faking received result')
            yield [
                EventMessage(api_name='my_company.auth',
                             event_name='user_registered', kwargs={'example': 'value'})
            ]

    async def start_listening_for(self, api_name, event_name):
        logger.info('Beginning to listen for {}.{}'.format(api_name, event_name))
        self._events.add('{}.{}'.format(api_name, event_name))
        if self._task:
            logger.debug('Existing consumer task running, cancelling')
            self._task.cancel()

    async def stop_listening_for(self, api_name, event_name):
        pass
