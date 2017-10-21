import logging
from typing import Any, Optional, Callable

import asyncio

import time

from lightbus.exceptions import InvalidEventArguments, InvalidBusNodeConfiguration, UnknownApi, EventNotFound
from lightbus.log import LBullets, L, Bold
from lightbus.message import RpcMessage, ResultMessage, EventMessage
from lightbus.api import registry
from lightbus.transports import RpcTransport, ResultTransport, EventTransport, RedisRpcTransport, \
    RedisResultTransport, RedisEventTransport
from lightbus.utilities import handle_aio_exceptions, human_time, block

__all__ = ['Bus', 'BusNode', 'create']


logger = logging.getLogger(__name__)


class Bus(object):
    # TODO: Rename from Bus to something else. This is more of a coordinator,
    #       and the BusNodes will be presented as the actual 'bus'

    def __init__(self, rpc_transport: 'RpcTransport', result_transport: 'ResultTransport',
                 event_transport: 'EventTransport'):
        self.rpc_transport = rpc_transport
        self.result_transport = result_transport
        self.event_transport = event_transport
        self._listeners = {}

    def run_forever(self, loop=None):
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
            logger.warning(
                "No APIs have been registered, lightbus may still receive events "
                "but it will not handle any incoming RPCs"
            )

        loop = loop or asyncio.get_event_loop()

        if registry.all():
            asyncio.ensure_future(handle_aio_exceptions(self.consume_rpcs), loop=loop)
        asyncio.ensure_future(handle_aio_exceptions(self.consume_events), loop=loop)

        try:
            loop.run_forever()
        except KeyboardInterrupt:
            logger.error('Keyboard interrupt. Shutting down...')
        finally:
            for task in asyncio.Task.all_tasks():
                task.cancel()

    # RPCs

    async def consume_rpcs(self, apis=None):
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
        return_path = self.result_transport.get_return_path(rpc_message)
        rpc_message.return_path = return_path

        logger.info("➡ Calling remote RPC ".format(rpc_message))

        start_time = time.time()
        # TODO: It is possible that the RPC will be called before we start waiting for the response. This is bad.
        result, _ = await asyncio.wait_for(asyncio.gather(
            self.result_transport.receive_result(rpc_message, return_path),
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

    async def consume_events(self):
        while True:
            event_messages = await self.event_transport.consume_events()
            for event_message in event_messages:
                key = (event_message.api_name, event_message.event_name)
                for listener in self._listeners.get(key, []):
                    listener(**event_message.kwargs)

    async def on_fire(self, api_name, name, kwargs: dict):
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

        event_message = EventMessage(api_name=api.meta.name, event_name=name, kwargs=kwargs)
        await self.event_transport.send_event(event_message)

    async def on_listen(self, api_name, name, listener):
        key = (api_name, name)
        self._listeners.setdefault(key, [])
        self._listeners[key].append(listener)
        await self.event_transport.start_listening_for(api_name, name)

    # Results

    async def send_result(self, rpc_message: RpcMessage, result: Any):
        result_message = ResultMessage(result=result)
        return await self.result_transport.send_result(rpc_message, result_message)

    async def receive_result(self, rpc_message: RpcMessage):
        return await self.result_transport.receive_result(rpc_message)


class BusNode(object):

    def __init__(self, name: str, *, parent: Optional['BusNode'], bus: Bus):
        if not parent and name:
            raise InvalidBusNodeConfiguration("Root client node may not have a name")
        self.name = name
        self.parent = parent
        self.bus = bus

    def __getattr__(self, item):
        return BusNode(name=item, parent=self, bus=self.bus)

    def __str__(self):
        return self.fully_qualified_name

    def __repr__(self):
        return '<BusNode {}>'.format(self.fully_qualified_name)

    def __call__(self, **kwargs):
        return block(self.asyn(), timeout=1)

    async def asyn(self, **kwargs):
        return await self.bus.call_rpc_remote(api_name=self.api_name, name=self.name, kwargs=kwargs)

    async def listen_asyn(self, listener):
        return await self.bus.on_listen(api_name=self.api_name, name=self.name, listener=listener)

    def listen(self, listener):
        return block(self.listen_asyn(listener), timeout=5)

    async def fire_asyn(self, **kwargs):
        return await self.bus.on_fire(api_name=self.api_name, name=self.name, kwargs=kwargs)

    def fire(self, **kwargs):
        return block(self.fire_asyn(**kwargs), timeout=5)

    def ancestors(self, include_self=False):
        parent = self
        while parent is not None:
            if parent != self or include_self:
                yield parent
            parent = parent.parent

    def run_forever(self, loop=None):
        self.bus.run_forever(loop=loop)

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


def create(
        rpc_transport: Optional['RpcTransport'] = None,
        result_transport: Optional['ResultTransport'] = None,
        event_transport: Optional['EventTransport'] = None,
        bus_class=Bus,
        **kwargs) -> BusNode:

    coordinator = bus_class(
        rpc_transport=rpc_transport or RedisRpcTransport(),
        result_transport=result_transport or RedisResultTransport(),
        event_transport=event_transport or RedisEventTransport(),
        **kwargs
    )
    return BusNode(name='', parent=None, bus=coordinator)
