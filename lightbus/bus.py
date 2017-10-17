import logging
from typing import Any, Optional, Callable

import asyncio

import time

from lightbus.exceptions import InvalidEventArguments, InvalidBusNodeConfiguration, UnknownApi, EventNotFound
from lightbus.log import LBullets, L, Bold
from lightbus.message import RpcMessage, ResultMessage
from lightbus.api import registry
from lightbus.transports import RpcTransport, ResultTransport
from lightbus.utilities import handle_aio_exceptions, human_time

__all__ = ['Bus']


logger = logging.getLogger(__name__)


class Bus(object):

    def __init__(self, rpc_transport: 'RpcTransport', result_transport: 'ResultTransport',
                 event_transport: 'event_transport'):
        self.rpc_transport = rpc_transport
        self.result_transport = result_transport
        self.event_transport = event_transport

    def client(self):
        return BusNode(name='', parent=None,
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
                "Event transport": L(
                    '{}.{}', self.event_transport.__module__,
                    Bold(self.event_transport.__class__.__name__)
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
            self.result_transport.receive_result(rpc_message),
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

    def on_fire(self, api_name, name, kwargs: dict):
        try:
            api = registry.get(api_name)
        except UnknownApi:
            raise UnknownApi(
                "I tried to fire the event {api_name}.{name}, but could not find API {api_name} in the registry. "
                "An API being in the registry implies you are an authority on that API. Therefore, "
                "I require the API to be in the registry as it seems like a bad idea to fire "
                "events on behalf of remote APIs. However, this could also be caused by a typo in the "
                "API name or event name, or be because the API has not been "
                "correctly registered.".format(**locals())
            )

        try:
            event = api.get_event(name)
        except EventNotFound:
            raise EventNotFound(
                "I tried to fire the event {api_name}.{name}, but the API {api_name} does not "
                "seem to contain an event named {name}. You may need to define the event, you "
                "may also be using the incorrect API. Also check for typos.".format(**locals())
            )

        if set(event.arguments) != set(kwargs.keys()):
            raise InvalidEventArguments(
                "Invalid event arguments supplied when firing event. Attempted to fire event with "
                "{} arguments: {}. Event expected {}: {}".format(
                    len(kwargs), sorted(kwargs.keys()),
                    len(event.arguments), sorted(event.arguments),
                )
            )

        return self.event_transport.send_event(api, name, kwargs)

    def on_listen(self, api_name, name, listener):
        raise NotImplementedError()

    async def consume_events(self, api):
        return await self.rpc_transport.consume_events(api)

    # Results

    async def send_result(self, rpc_message: RpcMessage, result: Any):
        result_message = ResultMessage(result=result)
        return await self.result_transport.send_result(rpc_message, result_message)

    async def receive_result(self, rpc_message: RpcMessage):
        return await self.result_transport.receive_result(rpc_message)


class BusNode(object):

    def __init__(self, name: str, *, parent: Optional['BusNode'],
                 on_call: Callable, on_listen: Callable, on_fire: Callable):
        if not parent and name:
            raise InvalidBusNodeConfiguration("Root client node may not have a name")
        self.name = name
        self.parent = parent
        self.on_call = on_call
        self.on_listen = on_listen
        self.on_fire = on_fire

    def __getattr__(self, item):
        return BusNode(name=item, parent=self,
                       on_call=self.on_call, on_listen=self.on_listen, on_fire=self.on_fire)

    def __str__(self):
        return self.fully_qualified_name

    def __repr__(self):
        return '<BusNode {}>'.format(self.fully_qualified_name)

    def __call__(self, **kwargs):
        coroutine = self.on_call(api_name=self.api_name, name=self.name, kwargs=kwargs)

        # TODO: Make some utility code for this
        loop = asyncio.get_event_loop()
        val = loop.run_until_complete(asyncio.wait_for(coroutine, timeout=1))
        return val

    async def asyn(self, **kwargs):
        return await self.on_call(api_name=self.api_name, name=self.name, kwargs=kwargs)

    def listen(self, listener):
        return self.on_listen(api_name=self.api_name, name=self.name, listener=listener)

    def fire(self, **kwargs):
        return self.on_fire(api_name=self.api_name, name=self.name, kwargs=kwargs)

    def ancestors(self, include_self=False):
        parent = self
        while parent is not None:
            if parent != self or include_self:
                yield parent
            parent = parent.parent

    @property
    def api_name(self):
        path = [node.name for node in self.ancestors(include_self=False)]
        path.reverse()
        return '.'.join(path[1:])

    @property
    def fully_qualified_name(self):
        path = [node.name for node in reversed(self.ancestors(include_self=True))]
        path.reverse()
        return '.'.join(path[1:])
