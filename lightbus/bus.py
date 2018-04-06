import os
import asyncio
import contextlib
import inspect
import logging
import signal
import time
from asyncio.futures import CancelledError
from typing import Optional, List, Sequence, Tuple

from lightbus.config import Config
from lightbus.schema import Schema
from lightbus.api import registry, Api
from lightbus.exceptions import InvalidEventArguments, InvalidBusNodeConfiguration, UnknownApi, EventNotFound, \
    InvalidEventListener, SuddenDeathException, LightbusTimeout, LightbusServerError, NoApisToListenOn, InvalidName, \
    InvalidParameters, ApisMustUseSameTransport, OnlyAvailableOnRootNode
from lightbus.internal_apis import LightbusStateApi, LightbusMetricsApi
from lightbus.log import LBullets, L, Bold
from lightbus.message import RpcMessage, ResultMessage, EventMessage, Message
from lightbus.plugins import autoload_plugins, plugin_hook, manually_set_plugins
from lightbus.schema.schema import _parameter_names
from lightbus.transports import RpcTransport, ResultTransport, EventTransport, RedisRpcTransport, \
    RedisResultTransport, RedisEventTransport
from lightbus.transports.base import SchemaTransport, TransportRegistry
from lightbus.transports.redis import RedisSchemaTransport
from lightbus.utilities.frozendict import frozendict
from lightbus.utilities.human import human_time, generate_process_name
from lightbus.utilities.async import handle_aio_exceptions, block, get_event_loop, cancel

__all__ = ['BusClient', 'BusNode', 'create']


logger = logging.getLogger(__name__)


class BusClient(object):

    def __init__(self,
                 config: 'Config',
                 transport_registry: TransportRegistry=None,
                 process_name: str='',
                 loop: asyncio.AbstractEventLoop=None):

        self.config = config
        self.transport_registry = transport_registry or TransportRegistry().load_config(config)
        # TODO: Schema transport should not be in registry
        self.schema = Schema(schema_transport=self.transport_registry.get_schema_transport('default'))
        self.process_name = process_name or generate_process_name()
        self.loop = loop or get_event_loop()
        self._listeners = {}

    def setup(self, plugins: dict=None):
        """Setup lightbus and get it ready to consume events and/or RPCs

        You should call this manually if you are calling `consume_rpcs()`
        directly. This you be handled for you if you are
        calling `run_forever()`.
        """
        if plugins is None:
            logger.debug("Auto-loading any installed Lightbus plugins...")
            plugins = autoload_plugins(self.config)
        else:
            logger.debug("Loading explicitly specified Lightbus plugins....")
            manually_set_plugins(plugins)

        if plugins:
            logger.info(LBullets("Loaded the following plugins ({})".format(len(plugins)), items=plugins))
        else:
            logger.info("No plugins loaded")

        # Load schema
        logger.debug("Loading schema...")
        block(self.schema.load_from_bus(), self.loop, timeout=5)  # config: schema_load_timeout
        for api in registry.all():
            block(self.schema.add_api(api), self.loop, timeout=1)  # config: schema_add_api_timeout

        logger.info(LBullets(
            "Loaded the following remote schemas ({})".format(len(self.schema.remote_schemas)),
            items=self.schema.remote_schemas.keys()
        ))

    def close(self):
        block(self.close_async(), loop=self.loop, timeout=5)

    async def close_async(self):
        listener_tasks = [
            task
            for task
            in asyncio.Task.all_tasks(loop=self.loop)
            if getattr(task, 'is_listener', False)
        ]
        await cancel(*listener_tasks)

        for transport in self.transport_registry.get_all_transports():
            await transport.close()

    def run_forever(self, *, consume_rpcs=True, plugins=None):
        rpc_transport = self.transport_registry.get_rpc_transport('default', default=None)
        result_transport = self.transport_registry.get_result_transport('default', default=None)
        event_transport = self.transport_registry.get_event_transport('default', default=None)

        logger.info(LBullets(
            "Lightbus getting ready to run. Default transports are:",
            items={
                "RPC transport": L(
                    '{}.{}',
                    rpc_transport.__module__, Bold(rpc_transport.__class__.__name__)
                ),
                "Result transport": L(
                    '{}.{}', result_transport.__module__,
                    Bold(result_transport.__class__.__name__)
                ) if rpc_transport else None,
                "Event transport": L(
                    '{}.{}', event_transport.__module__,
                    Bold(event_transport.__class__.__name__)
                ) if rpc_transport else None,
                "Schema transport": L(
                    '{}.{}', self.schema.schema_transport.__module__,
                    Bold(self.schema.schema_transport.__class__.__name__)
                ) if rpc_transport else None,
            }
        ))

        self.setup(plugins=plugins)
        registry.add(LightbusStateApi())
        registry.add(LightbusMetricsApi())

        if consume_rpcs:
            logger.info(LBullets(
                "APIs in registry ({})".format(len(registry.all())),
                items=registry.names()
            ))

        block(plugin_hook('before_server_start', bus_client=self), self.loop, timeout=5)

        self._run_forever(consume_rpcs)

        self.loop.run_until_complete(plugin_hook('after_server_stopped', bus_client=self))

    def _run_forever(self, consume_rpcs):
        # Setup RPC consumption
        consume_rpc_task = None
        if consume_rpcs and registry.all():
            consume_rpc_task = asyncio.ensure_future(self.consume_rpcs(), loop=self.loop)

        # Setup schema monitoring
        monitor_task = asyncio.ensure_future(self.schema.monitor(), loop=self.loop)

        self.loop.add_signal_handler(signal.SIGINT, self.loop.stop)
        self.loop.add_signal_handler(signal.SIGTERM, self.loop.stop)

        try:
            self.loop.run_forever()
            logger.error('Interrupt received. Shutting down...')
        except KeyboardInterrupt:
            logger.error('Keyboard interrupt. Shutting down...')

        # The loop has stopped, so we're shutting down

        # Remove the signal handlers
        self.loop.remove_signal_handler(signal.SIGINT)
        self.loop.remove_signal_handler(signal.SIGTERM)

        # Cancel the tasks we created above
        block(
            cancel(consume_rpc_task, monitor_task),
            loop=self.loop, timeout=1
        )

        # Close the bus (which will in turn close the transports)
        self.close()

    # RPCs

    async def consume_rpcs(self, apis: List[Api]=None):
        if apis is None:
            apis = registry.all()

        if not apis:
            raise NoApisToListenOn(
                'No APIs to consume on in consume_rpcs(). Either this method was called with apis=[], '
                'or the API registry is empty.'
            )

        while True:
            # Not all APIs will necessarily be served by the same transport, so group them
            # accordingly
            api_names = [api.meta.name for api in apis]
            api_names_by_transport = self.transport_registry.get_rpc_transports(api_names)

            coroutines = []
            for rpc_transport, transport_api_names in api_names_by_transport:
                transport_apis = list(map(registry.get, transport_api_names))
                coroutines.append(
                    self._consume_rpcs_with_transport(
                        rpc_transport=rpc_transport,
                        apis=transport_apis
                    )
                )

            await asyncio.gather(*coroutines)

    async def _consume_rpcs_with_transport(self, rpc_transport: RpcTransport, apis: List[Api] = None):
        rpc_messages = await rpc_transport.consume_rpcs(apis)
        for rpc_message in rpc_messages:
            self._validate(rpc_message, 'incoming')

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

                self._validate(result_message, 'outgoing',
                               api_name=rpc_message.api_name, procedure_name=rpc_message.procedure_name)

                await self.send_result(rpc_message=rpc_message, result_message=result_message)

    async def call_rpc_remote(self, api_name: str, name: str, kwargs: dict=frozendict(), options: dict=frozendict()):
        rpc_transport = self.transport_registry.get_rpc_transport(api_name)
        result_transport = self.transport_registry.get_result_transport(api_name)

        rpc_message = RpcMessage(api_name=api_name, procedure_name=name, kwargs=kwargs)
        return_path = result_transport.get_return_path(rpc_message)
        rpc_message.return_path = return_path
        options = options or {}
        timeout = options.get('timeout', 5)  # config: rpc_timeout

        self._validate_name(api_name, 'rpc', name)

        logger.info("📞  Calling remote RPC {}.{}".format(Bold(api_name), Bold(name)))

        start_time = time.time()
        # TODO: It is possible that the RPC will be called before we start waiting for the response. This is bad.

        self._validate(rpc_message, 'outgoing')

        future = asyncio.gather(
            self.receive_result(rpc_message, return_path, options=options),
            rpc_transport.call_rpc(rpc_message, options=options),
        )

        await plugin_hook('before_rpc_call', rpc_message=rpc_message, bus_client=self)

        try:
            result_message, _ = await asyncio.wait_for(future, timeout=timeout)
            future.result()
        except asyncio.TimeoutError:
            # Allow the future to finish, as per https://bugs.python.org/issue29432
            try:
                await future
                future.result()
            except CancelledError:
                pass

            # TODO: Include description of possible causes and how to increase the timeout.
            # TODO: Remove RPC from queue. Perhaps add a RpcBackend.cancel() method. Optional,
            #       as not all backends will support it. No point processing calls which have timed out.
            raise LightbusTimeout('Timeout when calling RPC {} after {} seconds'.format(
                rpc_message.canonical_name, timeout
            )) from None

        await plugin_hook('after_rpc_call', rpc_message=rpc_message, result_message=result_message, bus_client=self)

        if not result_message.error:
            logger.info(L("🏁  Remote call of {} completed in {}", Bold(rpc_message.canonical_name), human_time(time.time() - start_time)))
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

        self._validate(result_message, 'incoming', api_name, procedure_name=name)

        return result_message.result

    async def call_rpc_local(self, api_name: str, name: str, kwargs: dict=frozendict()):
        api = registry.get(api_name)
        self._validate_name(api_name, 'rpc', name)

        start_time = time.time()
        try:
            result = await api.call(name, kwargs)
        except (CancelledError, SuddenDeathException):
            raise
        except Exception as e:
            logger.warning(L("⚡  Error while executing {}.{}. Took {}", Bold(api_name), Bold(name), human_time(time.time() - start_time)))
            return e
        else:
            logger.info(L("⚡  Executed {}.{} in {}", Bold(api_name), Bold(name), human_time(time.time() - start_time)))
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

        self._validate_name(api_name, 'event', name)

        try:
            event = api.get_event(name)
        except EventNotFound:
            raise EventNotFound(
                "Lightbus tried to fire the event {api_name}.{name}, but the API {api_name} does not "
                "seem to contain an event named {name}. You may need to define the event, you "
                "may also be using the incorrect API. Also check for typos.".format(**locals())
            )

        if set(kwargs.keys()) != _parameter_names(event.parameters):
            raise InvalidEventArguments(
                "Invalid event arguments supplied when firing event. Attempted to fire event with "
                "{} arguments: {}. Event expected {}: {}".format(
                    len(kwargs), sorted(kwargs.keys()),
                    len(event.parameters), sorted(event.parameters),
                )
            )

        event_message = EventMessage(api_name=api.meta.name, event_name=name, kwargs=kwargs)

        self._validate(event_message, 'outgoing')

        event_transport = self.transport_registry.get_event_transport(api_name)
        await plugin_hook('before_event_sent', event_message=event_message, bus_client=self)
        logger.info(L("📤  Sending event {}.{}".format(Bold(api_name), Bold(name))))
        await event_transport.send_event(event_message, options=options)
        await plugin_hook('after_event_sent', event_message=event_message, bus_client=self)

    async def listen_for_event(self, api_name, name, listener, options: dict = None) -> asyncio.Task:
        return await self.listen_for_events([(api_name, name)], listener, options)

    async def listen_for_events(self,
                                events: List[Tuple[str, str]],
                                listener,
                                options: dict=None) -> asyncio.Task:

        if not callable(listener):
            raise InvalidEventListener(
                "The specified listener '{}' is not callable. Perhaps you called the function rather "
                "than passing the function itself?".format(listener)
            )

        for api_name, name in events:
            self._validate_name(api_name, 'event', name)

        options = options or {}
        listener_context = {}

        async def listen_for_event_task(event_transport, events):
            # event_transport.consume() returns an asynchronous generator
            # which will provide us with messages
            consumer = event_transport.consume(
                listen_for=events,
                context=listener_context,
                **options
            )
            with self._register_listener(events):
                async for event_message in consumer:
                    logger.info(L("📩  Received event {}.{}".format(
                        Bold(event_message.api_name), Bold(event_message.event_name)
                    )))

                    self._validate(event_message, 'incoming')

                    await plugin_hook('before_event_execution', event_message=event_message, bus_client=self)

                    # Call the listener
                    co = listener(**event_message.kwargs)

                    # Await it if necessary
                    if inspect.isawaitable(co):
                        await co

                    # Let the event transport know that it can consider the
                    # event message to have been successfully consumed
                    await event_transport.consumption_complete(event_message, listener_context)
                    await plugin_hook('after_event_execution', event_message=event_message, bus_client=self)

        # Get the events transports for the selection of APIs that we are listening on
        event_transports = self.transport_registry.get_event_transports(
            api_names=[api_name for api_name, _ in events]
        )

        tasks = []
        for _event_transport, _api_names in event_transports:
            # Create a listener task for each event transport,
            # passing each a list of events for which it should listen
            _events = [
                (api_name, event_name)
                for api_name, event_name in events
                if api_name in _api_names
            ]
            tasks.append(
                handle_aio_exceptions(listen_for_event_task(_event_transport, _events)),
            )

        listener_task = asyncio.gather(*tasks)
        listener_task.is_listener = True  # Used by close()
        return listener_task

    # Results

    async def send_result(self, rpc_message: RpcMessage, result_message: ResultMessage):
        result_transport = self.transport_registry.get_result_transport(rpc_message.api_name)
        return await result_transport.send_result(rpc_message, result_message, rpc_message.return_path)

    async def receive_result(self, rpc_message: RpcMessage, return_path: str, options: dict):
        result_transport = self.transport_registry.get_result_transport(rpc_message.api_name)
        return await result_transport.receive_result(rpc_message, return_path, options)

    @contextlib.contextmanager
    def _register_listener(self, events: List[Tuple[str, str]]):
        """A context manager to help keep track of what the bus is listening for"""
        for api_name, event_name in events:
            key = (api_name, event_name)
            self._listeners.setdefault(key, 0)
            self._listeners[key] += 1

        yield

        for api_name, event_name in events:
            key = (api_name, event_name)
            self._listeners[key] -= 1
            if not self._listeners[key]:
                self._listeners.pop(key)

    def _validate(self, message: Message, direction: str, api_name=None, procedure_name=None):
        assert direction in ('incoming', 'outgoing')

        # Result messages do not carry the api or procedure name, so allow them to be
        # specified manually
        api_name = getattr(message, 'api_name', api_name)
        event_or_rpc_name = getattr(message, 'procedure_name', None) or getattr(message, 'event_name', procedure_name)
        api_config = self.config.api(api_name)
        strict_validation = api_config.strict_validation

        if not getattr(api_config.validate, direction):
            return

        if not strict_validation and api_name not in self.schema:
            logging.debug(
                f"Validation is enabled for API {api_name}, but there is no schema present for this API. "
                f"Validation is therefore not possible. You can force this to be an error by enabling "
                f"the 'strict_validation' config option. You can silence this message by disabling validation "
                f"for this API using the 'validate' option."
            )
            return

        if isinstance(message, (RpcMessage, EventMessage)):
            self.schema.validate_parameters(api_name, event_or_rpc_name, message.kwargs)
        elif isinstance(message, (ResultMessage)):
            self.schema.validate_response(api_name, event_or_rpc_name, message.result)

    # Utilities

    def _validate_name(self, api_name: str, type_: str, name: str):
        """Validate that the given RPC/event name is ok to use"""
        if not name:
            raise InvalidName(f"Empty {type_} name specified when calling API {api_name}")

        if name.startswith('_'):
            raise InvalidName(
                f"You can not use '{api_name}.{name}' as an {type_} because it starts with an underscore. "
                f"API attributes starting with underscores are not available on the bus."
            )


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
        # config: rpc_timeout
        return block(self.call_async(**kwargs, bus_options=bus_options), self.bus_client.loop, timeout=1)

    async def call_async(self, *args, bus_options=None, **kwargs):
        if args:
            raise InvalidParameters(
                f"You have attempted to call the RPC {self.fully_qualified_name} using positional "
                f"arguments. Lightbus requires you use keyword arguments. For example, "
                f"instead of func(1), use func(foo=1)."
            )
        return await self.bus_client.call_rpc_remote(
            api_name=self.api_name, name=self.name, kwargs=kwargs, options=bus_options
        )

    # Events

    async def listen_async(self, listener, *, bus_options: dict=None):
        return await self.bus_client.listen_for_event(
            api_name=self.api_name, name=self.name, listener=listener, options=bus_options
        )

    def listen(self, listener, *, bus_options: dict=None):
        # config: event_listener_setup_timeout
        return block(self.listen_async(listener, bus_options=bus_options), self.bus_client.loop, timeout=5)

    async def listen_multiple_async(self, events: List['BusNode'], listener, *, bus_options: dict = None):
        if self.parent:
            raise OnlyAvailableOnRootNode(
                'Both listen_multiple() and listen_multiple_async() are only available on the '
                'bus root. For example, call bus.listen_multiple(), not bus.my_api.my_event.listen_multiple()'
            )

        events = [(node.api_name, node.name) for node in events]
        return await self.bus_client.listen_for_events(
            events=events, listener=listener, options=bus_options
        )

    def listen_multiple(self, events: List['BusNode'], listener, *, bus_options: dict=None):
        return block(
            self.listen_multiple_async(events, listener, bus_options=bus_options), self.bus_client.loop, timeout=5
        )

    async def fire_async(self, *args, bus_options: dict=None, **kwargs):
        if args:
            raise InvalidParameters(
                f"You have attempted to fire the event {self.fully_qualified_name} using positional "
                f"arguments. Lightbus requires you use keyword arguments. For example, "
                f"instead of func(1), use func(foo=1)."
            )
        return await self.bus_client.fire_event(
            api_name=self.api_name, name=self.name, kwargs=kwargs, options=bus_options
        )

    def fire(self, *, bus_options: dict=None, **kwargs):
        # config: event_fire_timeout
        return block(self.fire_async(**kwargs, bus_options=bus_options), self.bus_client.loop, timeout=5)

    # Utilities

    def ancestors(self, include_self=False):
        parent = self
        while parent is not None:
            if parent != self or include_self:
                yield parent
            parent = parent.parent

    def run_forever(self, consume_rpcs=True, plugins=None):
        self.bus_client.run_forever(consume_rpcs=consume_rpcs, plugins=plugins)

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

    # Schema

    @property
    def schema(self):
        """Get the bus schema"""
        if self.parent is None:
            return self.bus_client.schema
        else:
            # TODO: Implement getting schema of child nodes if there is demand
            raise AttributeError(
                'Schema only available on root node. Use bus.schema, not bus.my_api.schema'
            )

    @property
    def parameter_schema(self):
        """Get the parameter JSON schema for the given event or RPC"""
        # TODO: Test
        return self.bus_client.schema.get_event_or_rpc_schema(self.api_name, self.name)['parameters']

    @property
    def response_schema(self):
        """Get the response JSON schema for the given RPC

        Only RPCs have responses. Accessing this property for an event will result in a
        SchemaNotFound error.
        """
        # TODO: Test
        rpc_schema = self.bus_client.schema.get_rpc_schema(self.api_name, self.name)['response']
        return rpc_schema['response']

    def validate_parameters(self, parameters: dict):
        # TODO: Test
        self.bus_client.schema.validate_parameters(self.api_name, self.name, parameters)

    def validate_response(self, response):
        # TODO: Test
        self.bus_client.schema.validate_parameters(self.api_name, self.name, response)


def create(
        rpc_transport: Optional['RpcTransport']=None,
        result_transport: Optional['ResultTransport']=None,
        event_transport: Optional['EventTransport']=None,
        schema_transport: Optional['SchemaTransport']=None,
        client_class=BusClient,
        node_class=BusNode,
        config: dict=frozenset(),
        plugins=None,
        loop: asyncio.AbstractEventLoop=None,
        flask: bool=False,
        **kwargs) -> BusNode:

    if flask and os.environ.get('WERKZEUG_RUN_MAIN', '').lower() != 'true':
        # Flask has a reloader process that shouldn't start a lightbus client
        return

    from lightbus.config import Config
    config = Config.load_dict(config or {})
    transport_registry = TransportRegistry().load_config(config)

    if not transport_registry.has_rpc_transport('default'):
        transport_registry.set_rpc_transport('default', rpc_transport or RedisRpcTransport())

    if not transport_registry.has_result_transport('default'):
        transport_registry.set_result_transport('default', result_transport or RedisResultTransport())

    if not transport_registry.has_event_transport('default'):
        transport_registry.set_event_transport('default', event_transport or RedisEventTransport())

    if not transport_registry.has_schema_transport('default'):
        transport_registry.set_schema_transport('default', schema_transport or RedisSchemaTransport())

    bus_client = client_class(
        transport_registry=transport_registry,
        config=config,
        loop=loop,
        **kwargs
    )
    bus_client.setup(plugins=plugins)

    return node_class(name='', parent=None, bus_client=bus_client)
