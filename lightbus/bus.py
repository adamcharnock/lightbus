import asyncio
import contextlib
import inspect
import logging
import time
import traceback
from asyncio.futures import CancelledError
from typing import Optional

from lightbus.api import registry
from lightbus.exceptions import InvalidEventArguments, InvalidBusNodeConfiguration, UnknownApi, EventNotFound, \
    InvalidEventListener, SuddenDeathException, LightbusTimeout, LightbusServerError
from lightbus.internal_apis import LightbusStateApi, LightbusMetricsApi
from lightbus.log import LBullets, L, Bold
from lightbus.message import RpcMessage, ResultMessage, EventMessage
from lightbus.plugins import autoload_plugins, plugin_hook, manually_set_plugins
from lightbus.transports import RpcTransport, ResultTransport, EventTransport, RedisRpcTransport, \
    RedisResultTransport, RedisEventTransport
from lightbus.utilities import handle_aio_exceptions, human_time, block, generate_process_name

__all__ = ['BusClient', 'BusNode', 'create']


logger = logging.getLogger(__name__)


class BusClient(object):

    def __init__(self, rpc_transport: 'RpcTransport', result_transport: 'ResultTransport',
                 event_transport: 'EventTransport', process_name: str=''):
        self.rpc_transport = rpc_transport
        self.result_transport = result_transport
        self.event_transport = event_transport
        self.process_name = process_name or generate_process_name()
        self._listeners = {}

    def setup(self, plugins: dict=None):
        """Setup lightbus and get it ready to consume events and/or RPCs

        You should call this manually if you are calling `consume_rpcs()`
        directly. This you be handled for you if you are
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

    def run_forever(self, *, loop=None, consume_rpcs=True, plugins=None):
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
            logger.info(LBullets(
                "APIs in registry ({})".format(len(registry.all())),
                items=registry.all()
            ))

        block(handle_aio_exceptions(
            plugin_hook('before_server_start', bus_client=self, loop=loop)
        ), timeout=5)

        loop = loop or asyncio.get_event_loop()
        self._run_forever(loop, consume_rpcs)

        loop.run_until_complete(handle_aio_exceptions(
            plugin_hook('after_server_stopped', bus_client=self, loop=loop)
        ))

    def _run_forever(self, loop, consume_rpcs):
        if consume_rpcs and registry.all():
            asyncio.ensure_future(handle_aio_exceptions(self.consume_rpcs()), loop=loop)

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
                    result_message = ResultMessage(result=result, rpc_id=rpc_message.rpc_id)
                    await plugin_hook('after_rpc_execution', rpc_message=rpc_message, result_message=result_message,
                                      bus_client=self)
                    await self.send_result(rpc_message=rpc_message, result_message=result_message)

    async def call_rpc_remote(self, api_name: str, name: str, kwargs: dict, options: dict):
        rpc_message = RpcMessage(api_name=api_name, procedure_name=name, kwargs=kwargs)
        return_path = self.result_transport.get_return_path(rpc_message)
        rpc_message.return_path = return_path
        options = options or {}
        timeout = options.get('timeout', 5)

        logger.info("➡ Calling remote RPC ".format(rpc_message))

        start_time = time.time()
        # TODO: It is possible that the RPC will be called before we start waiting for the response. This is bad.

        future = asyncio.gather(
            self.receive_result(rpc_message, return_path, options=options),
            self.rpc_transport.call_rpc(rpc_message, options=options),
        )

        await plugin_hook('before_rpc_call', rpc_message=rpc_message, bus_client=self)

        try:
            result_message, _ = await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            future.cancel()
            # TODO: Include description of possible causes and how to increase the timeout.
            # TODO: Remove RPC from queue. Perhaps add a RpcBackend.cancel() method. Optional,
            #       as not all backends will support it. No point processing calls which have timed out.
            raise LightbusTimeout('Timeout when calling RPC {} after {} seconds'.format(
                rpc_message.canonical_name, timeout
            )) from None

        await plugin_hook('after_rpc_call', rpc_message=rpc_message, result_message=result_message, bus_client=self)

        if not result_message.error:
            logger.info(L("⚡ Remote call of {} completed in {}", Bold(rpc_message.canonical_name), human_time(time.time() - start_time)))
        else:
            logger.warning(
                L("⚡ Server error during remote call of {}. Took {}: {}",
                  Bold(rpc_message.canonical_name),
                  human_time(time.time() - start_time),
                  result_message.result,
                ),
            )
            raise LightbusServerError('Error while calling {}: {}\nRemote stack trace:\n{}'.format(
                rpc_message.canonical_name,
                result_message.result,
                result_message.trace,
            ))

        return result_message.result

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

    async def fire_event(self, api_name, name, kwargs: dict=None, options: dict=None):
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

        if set(event.parameters) != set(kwargs.keys()):
            raise InvalidEventArguments(
                "Invalid event arguments supplied when firing event. Attempted to fire event with "
                "{} arguments: {}. Event expected {}: {}".format(
                    len(kwargs), sorted(kwargs.keys()),
                    len(event.parameters), sorted(event.parameters),
                )
            )

        event_message = EventMessage(api_name=api.meta.name, event_name=name, kwargs=kwargs)
        await plugin_hook('before_event_sent', event_message=event_message, bus_client=self)
        await self.event_transport.send_event(event_message, options=options)
        await plugin_hook('after_event_sent', event_message=event_message, bus_client=self)

    async def listen_for_event(self, api_name, name, listener, options: dict=None) -> asyncio.Task:
        if not callable(listener):
            raise InvalidEventListener(
                "The specified listener '{}' is not callable. Perhaps you called the function rather "
                "than passing the function itself?".format(listener)
            )
        options = options or {}
        listener_context = {}

        async def listen_for_event_task():
            # event_transport.consume() returns an asynchronous generator
            # which will provide us with messages
            consumer = self.event_transport.consume(
                listen_for=[(api_name, name)],
                context=listener_context,
                **options
            )
            with self._register_listener(api_name, name):
                async for event_message in consumer:
                    await plugin_hook('before_event_execution', event_message=event_message, bus_client=self)

                    # Call the listener
                    co = listener(**event_message.kwargs)

                    # Await it if necessary
                    if inspect.isawaitable(co):
                        await co

                    # Let the event transport know that it can consider the
                    # event message to have been successfully consumed
                    await self.event_transport.consumption_complete(event_message, listener_context)
                    await plugin_hook('after_event_execution', event_message=event_message, bus_client=self)

        return asyncio.ensure_future(
            handle_aio_exceptions(listen_for_event_task())
        )

    # Results

    async def send_result(self, rpc_message: RpcMessage, result_message: ResultMessage):
        return await self.result_transport.send_result(rpc_message, result_message, rpc_message.return_path)

    async def receive_result(self, rpc_message: RpcMessage, return_path: str, options: dict):
        return await self.result_transport.receive_result(rpc_message, return_path, options)

    @contextlib.contextmanager
    def _register_listener(self, api_name, event_name):
        """A context manager to help keep track of what the bus is listening for"""
        key = (api_name, event_name)
        self._listeners.setdefault(key, 0)
        self._listeners[key] += 1
        yield
        self._listeners[key] -= 1
        if not self._listeners[key]:
            self._listeners.pop(key)


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

    def __dir__(self):
        path = [node.name for node in self.ancestors(include_self=True)]
        path.reverse()

        api_names = [[''] + n.split('.') for n in registry.names()]

        matches = []
        apis = []
        for api_name in api_names:
            if api_name == path:
                # Api name matches exactly
                apis.append(api_name)
            elif api_name[:len(path)] == path:
                # Partial API match
                matches.append(api_name[len(path)])

        for api_name in apis:
            api = registry.get('.'.join(api_name[1:]))
            matches.extend(dir(api))

        return matches

    # RPC

    def __call__(self, **kwargs):
        return self.call(**kwargs)

    def call(self, *, bus_options=None, **kwargs):
        return block(self.call_async(**kwargs, bus_options=bus_options), timeout=1)

    async def call_async(self, *, bus_options=None, **kwargs):
        return await self.bus_client.call_rpc_remote(
            api_name=self.api_name, name=self.name, kwargs=kwargs, options=bus_options
        )

    # Events

    async def listen_async(self, listener, *, bus_options: dict=None):
        return await self.bus_client.listen_for_event(
            api_name=self.api_name, name=self.name, listener=listener, options=bus_options
        )

    def listen(self, listener, *, bus_options: dict=None):
        return block(self.listen_async(listener, bus_options=bus_options), timeout=5)

    async def fire_async(self, *, bus_options: dict=None, **kwargs):
        return await self.bus_client.fire_event(
            api_name=self.api_name, name=self.name, kwargs=kwargs, options=bus_options
        )

    def fire(self, *, bus_options: dict=None, **kwargs):
        return block(self.fire_async(**kwargs, bus_options=bus_options), timeout=5)

    # Utilities

    def ancestors(self, include_self=False):
        parent = self
        while parent is not None:
            if parent != self or include_self:
                yield parent
            parent = parent.parent

    def run_forever(self, loop=None, consume_rpcs=True, plugins=None):
        self.bus_client.run_forever(loop=loop, consume_rpcs=consume_rpcs, plugins=plugins)

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
        plugins=None,
        **kwargs) -> BusNode:

    bus_client = client_class(
        rpc_transport=rpc_transport or RedisRpcTransport(),
        result_transport=result_transport or RedisResultTransport(),
        event_transport=event_transport or RedisEventTransport(),
        **kwargs
    )
    bus_client.setup(plugins=plugins)
    return node_class(name='', parent=None, bus_client=bus_client)
