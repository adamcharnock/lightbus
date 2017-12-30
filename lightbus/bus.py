import logging
from asyncio.coroutines import CoroWrapper
from asyncio.futures import CancelledError
from datetime import datetime
from typing import Any, Optional, Callable

import asyncio

import time

from lightbus.exceptions import InvalidEventArguments, InvalidBusNodeConfiguration, UnknownApi, EventNotFound, \
    InvalidEventListener, SuddenDeathException, LightbusTimeout, LightbusServerError
from lightbus.internal_apis import LightbusStateApi, LightbusMetricsApi
from lightbus.log import LBullets, L, Bold
from lightbus.message import RpcMessage, ResultMessage, EventMessage
from lightbus.api import registry
from lightbus.plugins import autoload_plugins, plugin_hook, manually_set_plugins
from lightbus.transports import RpcTransport, ResultTransport, EventTransport, RedisRpcTransport, \
    RedisResultTransport, RedisEventTransport
from lightbus.utilities import handle_aio_exceptions, human_time, block

__all__ = ['BusClient', 'BusNode', 'create']


logger = logging.getLogger(__name__)


class BusClient(object):

    def __init__(self, rpc_transport: 'RpcTransport', result_transport: 'ResultTransport',
                 event_transport: 'EventTransport'):
        self.rpc_transport = rpc_transport
        self.result_transport = result_transport
        self.event_transport = event_transport
        self._listeners = {}

    def setup(self, plugins: dict=None):
        """Setup lightbus and get it ready to consume events and/or RPCs

        You should call this manually if you are calling `consume_rpcs()` or
        `consume_events()` directly. This you be handled for you if you are
        calling `run_forever()`.
        """
        if plugins is None:
            logger.debug("Auto-loading any installed Lightbus plugins...")
            plugins = autoload_plugins()
        else:
            logger.debug("Loading explicitly specified Lightbus plugins....")
            manually_set_plugins(plugins)

        if plugins:
            logger.info(LBullets("Loaded the following plugins ({})".format(len(plugins)), items=plugins))
        else:
            logger.info("No plugins loaded")

    def run_forever(self, *, loop=None, consume_rpcs=True, consume_events=True, plugins=None):
        logger.info(LBullets(
            "Lightbus getting ready to run. Brokers in use",
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

        self.setup(plugins=plugins)
        registry.add(LightbusStateApi())
        registry.add(LightbusMetricsApi())

        if consume_rpcs:
            if registry.public():
                logger.info(LBullets(
                    "APIs in registry ({})".format(len(registry.all())),
                    items=registry.all()
                ))
            else:
                if consume_events:
                    logger.warning(
                        "No APIs have been registered, lightbus may still receive events "
                        "but Lightbus will not handle any incoming RPCs"
                    )
                else:
                    logger.error(
                        "No APIs have been registered, yet Lightbus has been configured to only "
                        "handle RPCs. There is therefore nothing for lightbus to do. Exiting."
                    )
                    return


        loop = loop or asyncio.get_event_loop()
        loop.run_until_complete(handle_aio_exceptions(
            plugin_hook('before_server_start', bus_client=self, loop=loop)
        ))

        if consume_rpcs and registry.all():
            asyncio.ensure_future(handle_aio_exceptions(self.consume_rpcs()), loop=loop)
        if consume_events:
            asyncio.ensure_future(handle_aio_exceptions(self.consume_events()), loop=loop)

        try:
            loop.run_forever()
        except KeyboardInterrupt:
            logger.error('Keyboard interrupt. Shutting down...')
        finally:
            for task in asyncio.Task.all_tasks():
                task.cancel()

        loop.run_until_complete(handle_aio_exceptions(
            plugin_hook('after_server_stopped', bus_client=self, loop=loop)
        ))

    # RPCs

    async def consume_rpcs(self, apis=None):
        if apis is None:
            apis = registry.all()

        while True:
            rpc_messages = await self.rpc_transport.consume_rpcs(apis)
            for rpc_message in rpc_messages:
                await plugin_hook('before_rpc_execution', rpc_message=rpc_message, bus_client=self)
                try:
                    result = await self.call_rpc_local(
                        api_name=rpc_message.api_name,
                        name=rpc_message.procedure_name,
                        kwargs=rpc_message.kwargs
                    )
                except SuddenDeathException:
                    # Used to simulate message failure for testing
                    pass
                else:
                    await plugin_hook('after_rpc_execution', rpc_message=rpc_message, result=result, bus_client=self)
                    await self.send_result(rpc_message=rpc_message, result=result)

    async def call_rpc_remote(self, api_name: str, name: str, kwargs: dict, timeout=5):
        rpc_message = RpcMessage(api_name=api_name, procedure_name=name, kwargs=kwargs)
        return_path = self.result_transport.get_return_path(rpc_message)
        rpc_message.return_path = return_path

        logger.info("➡ Calling remote RPC ".format(rpc_message))

        start_time = time.time()
        # TODO: It is possible that the RPC will be called before we start waiting for the response. This is bad.

        future = asyncio.gather(
            self.result_transport.receive_result(rpc_message, return_path),
            self.rpc_transport.call_rpc(rpc_message),
            return_exceptions=True
        )

        await plugin_hook('before_rpc_call', rpc_message=rpc_message, bus_client=self)

        try:
            result, _ = await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            future.cancel()
            # TODO: Include description of possible causes and how to increase the timeout.
            # TODO: Remove RPC from queue. Perhaps add a RpcBackend.cancel() method. Optional,
            #       as not all backends will support it. No point processing calls which have timed out.
            raise LightbusTimeout('Timeout when calling RPC {} after {} seconds'.format(
                rpc_message.canonical_name, timeout
            )) from None

        await plugin_hook('after_rpc_call', rpc_message=rpc_message, result=result, bus_client=self)

        if not result.get('error'):
            logger.info(L("⚡ Remote call of {} completed in {}", Bold(rpc_message.canonical_name), human_time(time.time() - start_time)))
        else:
            logger.warning(
                L("⚡ Server error during remote call of {}. Took {}: {}",
                  Bold(rpc_message.canonical_name),
                  human_time(time.time() - start_time),
                  result.get('result'),
                ),
            )
            raise LightbusServerError('Error while calling {}: {}\nRemote stack trace:\n{}'.format(
                rpc_message.canonical_name,
                result.get('result'),
                result.get('trace'),
            ))

        return result.get('result')

    async def call_rpc_local(self, api_name: str, name: str, kwargs: dict):
        api = registry.get(api_name)
        start_time = time.time()
        try:
            result = await api.call(name, kwargs)
        except (CancelledError, SuddenDeathException):
            raise
        except Exception as e:
            logger.warning(L("⚡ Error while executing {}.{}. Took {}", Bold(api_name), Bold(name), human_time(time.time() - start_time)))
            return e
        else:
            logger.info(L("⚡ Executed {}.{} in {}", Bold(api_name), Bold(name), human_time(time.time() - start_time)))
            return result

    # Events

    async def consume_events(self):
        while True:
            await self._consume_events_once()

    async def _consume_events_once(self):
        try:
            async with self.event_transport.consume_events() as event_messages:
                for event_message in event_messages:
                    await plugin_hook('before_event_execution', event_message=event_message, bus_client=self)
                    key = (event_message.api_name, event_message.event_name)
                    for listener in self._listeners.get(key, []):
                        # TODO: Run in parallel/gathered?
                        co = listener(**event_message.kwargs)
                        if isinstance(co, (CoroWrapper, asyncio.Future)):
                            await co
                    await plugin_hook('after_event_execution', event_message=event_message, bus_client=self)

        except SuddenDeathException:
            # Useful for simulating crashes in testing.
            logger.info('Sudden death while holding {} messages'.format(len(event_messages)))
            return

    async def fire_event(self, api_name, name, kwargs: dict=None):
        kwargs = kwargs or {}
        try:
            api = registry.get(api_name)
        except UnknownApi:
            raise UnknownApi(
                "Lightbus tried to fire the event {api_name}.{name}, but could not find API {api_name} in the "
                "registry. An API being in the registry implies you are an authority on that API. Therefore, "
                "Lightbus requires the API to be in the registry as it is a bad idea to fire "
                "events on behalf of remote APIs. However, this could also be caused by a typo in the "
                "API name or event name, or be because the API class has not been "
                "imported. ".format(**locals())
            )

        try:
            event = api.get_event(name)
        except EventNotFound:
            raise EventNotFound(
                "Lightbus tried to fire the event {api_name}.{name}, but the API {api_name} does not "
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
        await plugin_hook('before_event_sent', event_message=event_message, bus_client=self)
        await self.event_transport.send_event(event_message)

    async def listen_for_event(self, api_name, name, listener):
        if not callable(listener):
            raise InvalidEventListener(
                "The specified listener '{}' is not callable. Perhaps you called the function rather "
                "than passing the function itself?".format(listener)
            )

        key = (api_name, name)
        self._listeners.setdefault(key, [])
        self._listeners[key].append(listener)
        await self.event_transport.start_listening_for(api_name, name)

    # Results

    async def send_result(self, rpc_message: RpcMessage, result: Any):
        result_message = ResultMessage(result=result)
        return await self.result_transport.send_result(rpc_message, result_message, rpc_message.return_path)

    async def receive_result(self, rpc_message: RpcMessage):
        return await self.result_transport.receive_result(rpc_message, rpc_message.return_path)


class BusNode(object):

    def __init__(self, name: str, *, parent: Optional['BusNode'], bus_client: BusClient):
        if not parent and name:
            raise InvalidBusNodeConfiguration("Root client node may not have a name")
        self.name = name
        self.parent = parent
        self.bus_client = bus_client

    def __getattr__(self, item) -> 'BusNode':
        return self.__class__(name=item, parent=self, bus_client=self.bus_client)

    def __str__(self):
        return self.fully_qualified_name

    def __repr__(self):
        return '<BusNode {}>'.format(self.fully_qualified_name)

    # RPC

    def __call__(self, **kwargs):
        return self.call(**kwargs)

    def call(self, **kwargs):
        return block(self.call_async(**kwargs), timeout=1)

    async def call_async(self, **kwargs):
        return await self.bus_client.call_rpc_remote(api_name=self.api_name, name=self.name, kwargs=kwargs)

    # Events

    async def listen_async(self, listener):
        return await self.bus_client.listen_for_event(api_name=self.api_name, name=self.name, listener=listener)

    def listen(self, listener):
        return block(self.listen_async(listener), timeout=5)

    async def fire_async(self, **kwargs):
        return await self.bus_client.fire_event(api_name=self.api_name, name=self.name, kwargs=kwargs)

    def fire(self, **kwargs):
        return block(self.fire_asyn(**kwargs), timeout=5)

    # Utilities

    def ancestors(self, include_self=False):
        parent = self
        while parent is not None:
            if parent != self or include_self:
                yield parent
            parent = parent.parent

    def run_forever(self, loop=None, consume_rpcs=True, consume_events=True, plugins=None):
        self.bus_client.run_forever(loop=loop, consume_rpcs=consume_rpcs, consume_events=consume_events, plugins=None)

    @property
    def api_name(self):
        path = [node.name for node in self.ancestors(include_self=False)]
        path.reverse()
        return '.'.join(path[1:])

    @property
    def fully_qualified_name(self):
        path = [node.name for node in self.ancestors(include_self=True)]
        path.reverse()
        return '.'.join(path[1:])


def create(
        rpc_transport: Optional['RpcTransport'] = None,
        result_transport: Optional['ResultTransport'] = None,
        event_transport: Optional['EventTransport'] = None,
        client_class=BusClient,
        node_class=BusNode,
        **kwargs) -> BusNode:

    bus_client = client_class(
        rpc_transport=rpc_transport or RedisRpcTransport(),
        result_transport=result_transport or RedisResultTransport(),
        event_transport=event_transport or RedisEventTransport(),
        **kwargs
    )
    return node_class(name='', parent=None, bus_client=bus_client)
