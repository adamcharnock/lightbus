from typing import Any

import asyncio

from lightbus.client import ClientNode
from lightbus.message import RpcMessage, ResultMessage
import lightbus
from lightbus.utilities import handle_aio_exceptions

__all__ = ['Bus']


class Bus(object):

    def __init__(self, broker_transport: 'lightbus.BrokerTransport', result_transport: 'lightbus.ResultTransport'):
        self.broker_transport = broker_transport
        self.result_transport = result_transport

    # def __getattr__(self, item):
    #     return ClientNode(name=str(item), bus=self, parent=None)

    def serve(self, api, loop=None):
        loop = loop or asyncio.get_event_loop()
        asyncio.ensure_future(handle_aio_exceptions(self.consume, api=api), loop=loop)
        loop.run_forever()
        loop.close()

    # RPCs

    async def consume(self, api):
        while True:
            rpc_message = await self.broker_transport.consume_rpcs(api)
            result = await self.call_rpc_local(api, name=rpc_message.procedure_name, kwargs=rpc_message.kwargs)
            await self.send_result(rpc_message=rpc_message, result=result)

    async def call_rpc_remote(self, api, name, kwargs, priority=0):
        result_info = self.result_transport.get_result_info(api, name, kwargs, priority)
        return await self.broker_transport.call_rpc(api, name, kwargs, result_info, priority)

    async def call_rpc_local(self, api, name, kwargs):
        return await api.call(name, kwargs)

    # Events

    async def send_event(self, api, name, kwargs):
        return await self.broker_transport.send_event(api, name, kwargs)

    async def consume_events(self, api):
        return await self.broker_transport.consume_events(api)

    # Results

    async def send_result(self, rpc_message: RpcMessage, result: Any):
        result_message = ResultMessage(result=result)
        return await self.result_transport.send(rpc_message, result_message)

    async def receive_result(self, rpc_message: RpcMessage):
        return await self.result_transport.receive(rpc_message)
