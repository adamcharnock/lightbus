from typing import Any

import asyncio

from lightbus.client import ClientNode
from lightbus.message import RpcMessage, ResultMessage
import lightbus
from lightbus.api import Api
from lightbus.utilities import handle_aio_exceptions

__all__ = ['Bus']


class Bus(object):

    def __init__(self, broker_transport: 'lightbus.BrokerTransport', result_transport: 'lightbus.ResultTransport'):
        self.broker_transport = broker_transport
        self.result_transport = result_transport

    def client(self):
        return ClientNode(name='', bus=self, parent=None)

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

    async def call_rpc_remote(self, api_name: str, name: str, kwargs: dict):
        rpc_message = RpcMessage(api_name=api_name, procedure_name=name, kwargs=kwargs)
        rpc_message.return_path = self.result_transport.get_return_path(rpc_message)

        # TODO: It is possible that the RPC will be called before we start waiting for the response. This is bad.
        result, _ = await asyncio.wait_for(asyncio.gather(
            self.result_transport.receive(rpc_message),
            self.broker_transport.call_rpc(rpc_message),
        ), timeout=10)
        return result

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
