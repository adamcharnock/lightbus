import logging
from typing import Any

import asyncio

import time

from lightbus.client import ClientNode
from lightbus.log import LBullets, L, Bold
from lightbus.message import RpcMessage, ResultMessage
from lightbus.api import registry
from lightbus.transports import RpcTransport, ResultTransport
from lightbus.utilities import handle_aio_exceptions, human_time

__all__ = ['Bus']


logger = logging.getLogger(__name__)


class Bus(object):

    def __init__(self, rpc_transport: 'RpcTransport', result_transport: 'ResultTransport'):
        self.rpc_transport = rpc_transport
        self.result_transport = result_transport

    def client(self):
        return ClientNode(name='', parent=None,
                          on_fire=self.on_fire, on_listen=self.on_listen, on_call=self.call_rpc_remote)

    def serve(self, api, loop=None):
        logger.info(LBullets(
            "Lightbus getting ready to serve. Brokers in use",
            items={
                "RPC transport": L(
                    '{}.{}',
                    self.rpc_transport.__module__, Bold(self.rpc_transport.__class__.__name__)
                ),
                "Result transport": L(
                    '{}.{}', self.result_transport.__module__,
                    Bold(self.result_transport.__class__.__name__)
                ),
            }
        ))

        if registry.all():
            logger.info(LBullets(
                "APIs in registry ({})".format(len(registry.all())),
                items=registry.all()
            ))
        else:
            logger.error("No APIs have been registered, lightbus has nothing to do. Exiting.")
            exit(1)

        loop = loop or asyncio.get_event_loop()

        asyncio.ensure_future(handle_aio_exceptions(self.consume), loop=loop)
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            logger.error('Keyboard interrupt. Shutting down...')
        finally:
            for task in asyncio.Task.all_tasks():
                task.cancel()

    # RPCs

    async def consume(self, apis=None):
        if apis is None:
            apis = registry.all()

        while True:
            rpc_messages = await self.rpc_transport.consume_rpcs(apis)
            for rpc_message in rpc_messages:
                result = await self.call_rpc_local(
                    api_name=rpc_message.api_name,
                    name=rpc_message.procedure_name,
                    kwargs=rpc_message.kwargs
                )
                await self.send_result(rpc_message=rpc_message, result=result)

    async def call_rpc_remote(self, api_name: str, name: str, kwargs: dict):
        rpc_message = RpcMessage(api_name=api_name, procedure_name=name, kwargs=kwargs)
        rpc_message.return_path = self.result_transport.get_return_path(rpc_message)

        logger.info("➡ Calling remote RPC ".format(rpc_message))

        start_time = time.time()
        # TODO: It is possible that the RPC will be called before we start waiting for the response. This is bad.
        result, _ = await asyncio.wait_for(asyncio.gather(
            self.result_transport.receive(rpc_message),
            self.rpc_transport.call_rpc(rpc_message),
        ), timeout=10)

        logger.info(L("⚡ Remote call of {} completed in {}", Bold(rpc_message.canonical_name), human_time(time.time() - start_time)))

        return result

    async def call_rpc_local(self, api_name: str, name: str, kwargs: dict):
        api = registry.get(api_name)
        start_time = time.time()
        result = await api.call(name, kwargs)
        logger.info(L("⚡ Executed {}.{} in {}", Bold(api_name), Bold(name), human_time(time.time() - start_time)))
        return result

    # Events

    def on_fire(self, api_name, name, kwargs):
        raise NotImplementedError()

    def on_listen(self, api_name, name, listener):
        raise NotImplementedError()

    async def send_event(self, api, name, kwargs):
        return await self.rpc_transport.send_event(api, name, kwargs)

    async def consume_events(self, api):
        return await self.rpc_transport.consume_events(api)

    # Results

    async def send_result(self, rpc_message: RpcMessage, result: Any):
        result_message = ResultMessage(result=result)
        return await self.result_transport.send(rpc_message, result_message)

    async def receive_result(self, rpc_message: RpcMessage):
        return await self.result_transport.receive(rpc_message)
