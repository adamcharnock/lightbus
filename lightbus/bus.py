from typing import Any

from lightbus.client import ClientNode
from lightbus.message import Message
from lightbus.server import Server
import lightbus

__all__ = ['Bus']


class Bus(object):

    def __init__(self, broker_transport: 'lightbus.BrokerTransport', result_transport: 'lightbus.ResultTransport'):
        self.broker_transport = broker_transport
        self.result_transport = result_transport

    def __getattr__(self, item):
        return ClientNode(name=str(item), bus=self, parent=None)

    def serve(self):
        Server(bus=self).run_forever()

    # RPCs

    async def call_rpc(self, api, name, kwargs, priority=0):
        result_info = self.result_transport.get_result_info(api, name, kwargs, priority)
        return await self.broker_transport.call_rpc(api, name, kwargs, result_info, priority)

    async def consume_rpcs(self, api):
        return await self.broker_transport.consume_rpcs(api)

    # Events

    async def send_event(self, api, name, kwargs):
        return await self.broker_transport.send_event(api, name, kwargs)

    async def consume_events(self, api):
        return await self.broker_transport.consume_events(api)

    # Results

    async def send_result(self, client_message: Message, result: Any):
        return await self.result_transport.send(client_message, result)

    async def receive_result(self, client_message: Message):
        return await self.result_transport.receive(client_message)
