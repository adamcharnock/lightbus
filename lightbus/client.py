import asyncio
import contextlib
import functools
import inspect
import logging
import signal
import time
from asyncio.futures import CancelledError
from collections import defaultdict
from typing import List, Tuple

from lightbus.api import registry, Api
from lightbus.config import Config
from lightbus.config.structure import OnError
from lightbus.exceptions import (
    InvalidEventArguments,
    UnknownApi,
    EventNotFound,
    InvalidEventListener,
    SuddenDeathException,
    LightbusTimeout,
    LightbusServerError,
    NoApisToListenOn,
    InvalidName,
    LightbusShutdownInProgress,
)
from lightbus.internal_apis import LightbusStateApi, LightbusMetricsApi
from lightbus.log import LBullets, L, Bold
from lightbus.message import RpcMessage, ResultMessage, EventMessage, Message
from lightbus.plugins import autoload_plugins, plugin_hook, manually_set_plugins
from lightbus.schema import Schema
from lightbus.schema.schema import _parameter_names
from lightbus.transports import RpcTransport
from lightbus.transports.base import TransportRegistry, EventTransport
from lightbus.utilities.async import (
    block,
    get_event_loop,
    cancel,
    await_if_necessary,
    make_exception_checker,
)
from lightbus.utilities.casting import cast_to_signature
from lightbus.utilities.deforming import deform_to_bus
from lightbus.utilities.frozendict import frozendict
from lightbus.utilities.human import human_time
from lightbus.utilities.logging import log_transport_information

__all__ = ["BusClient"]


logger = logging.getLogger(__name__)


class BusClient(object):
    """Provides a the lower level interface for accessing the bus

    The low-level `BusClient` is less expressive than the interface provided by `BusPath`,
    but does allow for more control in some situations.

    All functionality in `BusPath` is provided by `BusClient`.
    """

    def __init__(self, config: "Config", transport_registry: TransportRegistry = None):

        self.config = config
        self.transport_registry = transport_registry or TransportRegistry().load_config(config)
        self.schema = Schema(
            schema_transport=self.transport_registry.get_schema_transport("default"),
            max_age_seconds=self.config.bus().schema.ttl,
            human_readable=self.config.bus().schema.human_readable,
        )
        self._listeners = {}
        self._hook_callbacks = defaultdict(list)
        self._exit_code = 0

    async def setup_async(self, plugins: dict = None):
        """Setup lightbus and get it ready to consume events and/or RPCs

        You should call this manually if you are calling `consume_rpcs()`
        directly. This you be handled for you if you are
        calling `run_forever()`.
        """
        logger.info(
            LBullets(
                "Lightbus is setting up",
                items={
                    "service_name (set with -s or LIGHTBUS_SERVICE_NAME)": Bold(
                        self.config.service_name
                    ),
                    "process_name (with with -p or LIGHTBUS_PROCESS_NAME)": Bold(
                        self.config.process_name
                    ),
                },
            )
        )

        # Log the transport information
        rpc_transport = self.transport_registry.get_rpc_transport("default", default=None)
        result_transport = self.transport_registry.get_result_transport("default", default=None)
        event_transport = self.transport_registry.get_event_transport("default", default=None)
        log_transport_information(
            rpc_transport, result_transport, event_transport, self.schema.schema_transport, logger
        )

        # Log the plugins we have
        if plugins is None:
            logger.debug("Auto-loading any installed Lightbus plugins...")
            # Force auto-loading as many commands need to do config-less best-effort
            # plugin loading. But now we have the config loaded so we can
            # make sure we load the plugins properly.
            plugins = autoload_plugins(self.config, force=True)
        else:
            logger.debug("Loading explicitly specified Lightbus plugins....")
            manually_set_plugins(plugins)

        if plugins:
            logger.info(
                LBullets("Loaded the following plugins ({})".format(len(plugins)), items=plugins)
            )
        else:
            logger.info("No plugins loaded")

        # Load schema
        logger.debug("Loading schema...")
        await self.schema.load_from_bus()

        # Share the schema of the registered APIs
        for api in registry.all():
            await self.schema.add_api(api)

        logger.info(
            LBullets(
                "Loaded the following remote schemas ({})".format(len(self.schema.remote_schemas)),
                items=self.schema.remote_schemas.keys(),
            )
        )

        for transport in self.transport_registry.get_all_transports():
            await transport.open()

    def setup(self, plugins: dict = None):
        block(self.setup_async(plugins), timeout=5)

    def close(self):
        block(self.close_async(), timeout=5)

    async def close_async(self):
        listener_tasks = [
            task for task in asyncio.Task.all_tasks() if getattr(task, "is_listener", False)
        ]

        for task in listener_tasks:
            try:
                await cancel(task)
            except Exception as e:
                logger.exception(e)
                # The log message assumes that we are closing because of this error
                # (is this a safe assumption?)
                logger.error(
                    f"An event listener raised an exception while processing an event. "
                    f"Lightbus will now shutdown because the on_error option is set to"
                    f" '{OnError.SHUTDOWN.value}'"
                )

        for transport in self.transport_registry.get_all_transports():
            await transport.close()

        await self.schema.schema_transport.close()

    @property
    def loop(self):
        return get_event_loop()

    def run_forever(self, *, consume_rpcs=True):
        registry.add(LightbusStateApi())
        registry.add(LightbusMetricsApi())

        if consume_rpcs:
            logger.info(
                LBullets(
                    "APIs in registry ({})".format(len(registry.all())), items=registry.names()
                )
            )

        block(self._plugin_hook("before_server_start"), timeout=5)

        self._run_forever(consume_rpcs)

        self.loop.run_until_complete(self._plugin_hook("after_server_stopped"))

        # Close the bus (which will in turn close the transports)
        self.close()

        # See if we've set the exit code on the event loop
        if hasattr(self.loop, "lightbus_exit_code"):
            raise SystemExit(self.loop.lightbus_exit_code)

    def _run_forever(self, consume_rpcs):
        # Setup RPC consumption
        consume_rpc_task = None
        if consume_rpcs and registry.all():
            consume_rpc_task = asyncio.ensure_future(self.consume_rpcs())

        # Setup schema monitoring
        monitor_task = asyncio.ensure_future(self.schema.monitor())

        self.loop.add_signal_handler(signal.SIGINT, self.loop.stop)
        self.loop.add_signal_handler(signal.SIGTERM, self.loop.stop)

        try:
            self._actually_run_forever()
            logger.warning("Interrupt received. Shutting down...")
        except KeyboardInterrupt:
            logger.warning("Keyboard interrupt. Shutting down...")

        # The loop has stopped, so we're shutting down

        # Remove the signal handlers
        self.loop.remove_signal_handler(signal.SIGINT)
        self.loop.remove_signal_handler(signal.SIGTERM)

        # Cancel the tasks we created above
        block(cancel(consume_rpc_task, monitor_task), timeout=1)

    def _actually_run_forever(self):  # pragma: no cover
        """Simply start the loop running forever

        This just makes testing easier as we can mock out this method
        """
        self.loop.run_forever()

    # RPCs

    async def consume_rpcs(self, apis: List[Api] = None):
        if apis is None:
            apis = registry.all()

        if not apis:
            raise NoApisToListenOn(
                "No APIs to consume on in consume_rpcs(). Either this method was called with apis=[], "
                "or the API registry is empty."
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
                        rpc_transport=rpc_transport, apis=transport_apis
                    )
                )

            await asyncio.gather(*coroutines)

    async def _consume_rpcs_with_transport(
        self, rpc_transport: RpcTransport, apis: List[Api] = None
    ):
        rpc_messages = await rpc_transport.consume_rpcs(apis)
        for rpc_message in rpc_messages:
            self._validate(rpc_message, "incoming")

            await self._plugin_hook("before_rpc_execution", rpc_message=rpc_message)
            try:
                result = await self.call_rpc_local(
                    api_name=rpc_message.api_name,
                    name=rpc_message.procedure_name,
                    kwargs=rpc_message.kwargs,
                )
            except SuddenDeathException:
                # Used to simulate message failure for testing
                pass
            else:
                result = deform_to_bus(result)
                result_message = ResultMessage(result=result, rpc_message_id=rpc_message.id)
                await self._plugin_hook(
                    "after_rpc_execution", rpc_message=rpc_message, result_message=result_message
                )

                self._validate(
                    result_message,
                    "outgoing",
                    api_name=rpc_message.api_name,
                    procedure_name=rpc_message.procedure_name,
                )

                await self.send_result(rpc_message=rpc_message, result_message=result_message)

    async def call_rpc_remote(
        self, api_name: str, name: str, kwargs: dict = frozendict(), options: dict = frozendict()
    ):
        rpc_transport = self.transport_registry.get_rpc_transport(api_name)
        result_transport = self.transport_registry.get_result_transport(api_name)

        kwargs = deform_to_bus(kwargs)
        rpc_message = RpcMessage(api_name=api_name, procedure_name=name, kwargs=kwargs)
        return_path = result_transport.get_return_path(rpc_message)
        rpc_message.return_path = return_path
        options = options or {}
        timeout = options.get("timeout", self.config.api(api_name).rpc_timeout)

        self._validate_name(api_name, "rpc", name)

        logger.info("📞  Calling remote RPC {}.{}".format(Bold(api_name), Bold(name)))

        start_time = time.time()
        # TODO: It is possible that the RPC will be called before we start waiting for the response. This is bad.

        self._validate(rpc_message, "outgoing")

        future = asyncio.gather(
            self.receive_result(rpc_message, return_path, options=options),
            rpc_transport.call_rpc(rpc_message, options=options),
        )

        await self._plugin_hook("before_rpc_call", rpc_message=rpc_message)

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

            # TODO: Remove RPC from queue. Perhaps add a RpcBackend.cancel() method. Optional,
            #       as not all backends will support it. No point processing calls which have timed out.
            raise LightbusTimeout(
                f"Timeout when calling RPC {rpc_message.canonical_name} after {timeout} seconds. "
                f"It is possible no Lightbus process is serving this API, or perhaps it is taking "
                f"too long to process the request. In which case consider raising the 'rpc_timeout' "
                f"config option."
            ) from None

        await self._plugin_hook(
            "after_rpc_call", rpc_message=rpc_message, result_message=result_message
        )

        if not result_message.error:
            logger.info(
                L(
                    "🏁  Remote call of {} completed in {}",
                    Bold(rpc_message.canonical_name),
                    human_time(time.time() - start_time),
                )
            )
        else:
            logger.warning(
                L(
                    "⚡ Server error during remote call of {}. Took {}: {}",
                    Bold(rpc_message.canonical_name),
                    human_time(time.time() - start_time),
                    result_message.result,
                )
            )
            raise LightbusServerError(
                "Error while calling {}: {}\nRemote stack trace:\n{}".format(
                    rpc_message.canonical_name, result_message.result, result_message.trace
                )
            )

        self._validate(result_message, "incoming", api_name, procedure_name=name)

        return result_message.result

    async def call_rpc_local(self, api_name: str, name: str, kwargs: dict = frozendict()):
        api = registry.get(api_name)
        self._validate_name(api_name, "rpc", name)

        start_time = time.time()
        try:
            method = getattr(api, name)
            if self.config.api(api_name).cast_values:
                kwargs = cast_to_signature(kwargs, method)
            result = method(**kwargs)
            result = await await_if_necessary(result)
        except (CancelledError, SuddenDeathException):
            raise
        except Exception as e:
            logger.warning(
                L(
                    "⚡  Error while executing {}.{}. Took {}",
                    Bold(api_name),
                    Bold(name),
                    human_time(time.time() - start_time),
                )
            )
            return e
        else:
            logger.info(
                L(
                    "⚡  Executed {}.{} in {}",
                    Bold(api_name),
                    Bold(name),
                    human_time(time.time() - start_time),
                )
            )
            return result

    # Events

    async def fire_event(self, api_name, name, kwargs: dict = None, options: dict = None):
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

        self._validate_name(api_name, "event", name)

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
                    len(kwargs),
                    sorted(kwargs.keys()),
                    len(event.parameters),
                    sorted(_parameter_names(event.parameters)),
                )
            )

        kwargs = deform_to_bus(kwargs)
        event_message = EventMessage(api_name=api.meta.name, event_name=name, kwargs=kwargs)

        self._validate(event_message, "outgoing")

        event_transport = self.transport_registry.get_event_transport(api_name)
        await self._plugin_hook("before_event_sent", event_message=event_message)
        logger.info(L("📤  Sending event {}.{}".format(Bold(api_name), Bold(name))))
        await event_transport.send_event(event_message, options=options)
        await self._plugin_hook("after_event_sent", event_message=event_message)

    async def listen_for_event(
        self, api_name, name, listener, options: dict = None
    ) -> asyncio.Task:
        return await self.listen_for_events([(api_name, name)], listener, options)

    async def listen_for_events(
        self, events: List[Tuple[str, str]], listener, options: dict = None
    ) -> asyncio.Task:

        self._sanity_check_listener(listener)

        for api_name, name in events:
            self._validate_name(api_name, "event", name)

        event_listener = _EventListener(
            events=events, listener_callable=listener, options=options, bus_client=self
        )
        return event_listener.make_task()

    # Results

    async def send_result(self, rpc_message: RpcMessage, result_message: ResultMessage):
        result_transport = self.transport_registry.get_result_transport(rpc_message.api_name)
        return await result_transport.send_result(
            rpc_message, result_message, rpc_message.return_path
        )

    async def receive_result(self, rpc_message: RpcMessage, return_path: str, options: dict):
        result_transport = self.transport_registry.get_result_transport(rpc_message.api_name)
        return await result_transport.receive_result(rpc_message, return_path, options)

    @contextlib.contextmanager
    def _register_listener(self, events: List[Tuple[str, str]]):
        """A context manager to help keep track of what the bus is listening for"""
        logger.info(
            LBullets(f"Registering listener for", items=[Bold(f"{a}.{e}") for a, e in events])
        )

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

    @property
    def listeners(self) -> List[Tuple[str, str]]:
        """Returns a list of events which are currently being listened to

        Each value is a tuple in the form `(api_name, event_name)`.
        """
        return list(self._listeners.keys())

    def _validate(self, message: Message, direction: str, api_name=None, procedure_name=None):
        assert direction in ("incoming", "outgoing")

        # Result messages do not carry the api or procedure name, so allow them to be
        # specified manually
        api_name = getattr(message, "api_name", api_name)
        event_or_rpc_name = getattr(message, "procedure_name", None) or getattr(
            message, "event_name", procedure_name
        )
        api_config = self.config.api(api_name)
        strict_validation = api_config.strict_validation

        if not getattr(api_config.validate, direction):
            return

        if api_name not in self.schema:
            if strict_validation:
                raise UnknownApi(
                    f"Validation is enabled for API {api_name}, but there is no schema present for this API. "
                    f"Validation is therefore not possible. You are also seeing this error because the "
                    f"'strict_validation' setting is enabled. Disabling this setting will turn this exception "
                    f"into a warning. "
                )
            else:
                logger.warning(
                    f"Validation is enabled for API {api_name}, but there is no schema present for this API. "
                    f"Validation is therefore not possible. You can force this to be an error by enabling "
                    f"the 'strict_validation' config option. You can silence this message by disabling validation "
                    f"for this API using the 'validate' option."
                )
                return

        if isinstance(message, (RpcMessage, EventMessage)):
            self.schema.validate_parameters(api_name, event_or_rpc_name, message.kwargs)
        elif isinstance(message, ResultMessage):
            self.schema.validate_response(api_name, event_or_rpc_name, message.result)

    # Utilities

    def _validate_name(self, api_name: str, type_: str, name: str):
        """Validate that the given RPC/event name is ok to use"""
        if not name:
            raise InvalidName(f"Empty {type_} name specified when calling API {api_name}")

        if name.startswith("_"):
            raise InvalidName(
                f"You can not use '{api_name}.{name}' as an {type_} because it starts with an underscore. "
                f"API attributes starting with underscores are not available on the bus."
            )

    def _sanity_check_listener(self, listener):
        if not callable(listener):
            raise InvalidEventListener(
                f"The specified event listener {listener} is not callable. Perhaps you called the function rather "
                f"than passing the function itself?"
            )

        total_positional_args = 0
        has_variable_positional_args = False  # Eg: *args
        for parameter in inspect.signature(listener).parameters.values():
            if parameter.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            ):
                total_positional_args += 1
            elif parameter.kind == inspect.Parameter.VAR_POSITIONAL:
                has_variable_positional_args = True

        if has_variable_positional_args:
            return

        if not total_positional_args:
            raise InvalidEventListener(
                f"The specified event listener {listener} must take at one positional argument. "
                f"This will be the event message. For example: "
                f"my_listener(event_message, other, ...)"
            )

    async def _plugin_hook(self, name, **kwargs):
        # Hooks that need to run before plugins
        for callback in self._hook_callbacks[(name, True)]:
            await await_if_necessary(callback(client=self, **kwargs))

        await plugin_hook(name, client=self, **kwargs)

        # Hooks that need to run after plugins
        for callback in self._hook_callbacks[(name, False)]:
            await await_if_necessary(callback(client=self, **kwargs))

    def _register_hook_callback(self, name, fn, before_plugins=False):
        self._hook_callbacks[(name, bool(before_plugins))].append(fn)

    def _make_hook_decorator(self, name, before_plugins=False, callback=None):
        assert not callback or callable(callback), "The provided callback is not callable"
        if callback:
            self._register_hook_callback(name, callback, before_plugins)
        else:

            def hook_decorator(fn):
                self._register_hook_callback(name, fn, before_plugins)
                return fn

            return hook_decorator

    def on_start(self, callback=None, *, before_plugins=False):
        """Alias for before_server_start"""
        return self.before_server_start(callback, before_plugins=before_plugins)

    def on_stop(self, callback=None, *, before_plugins=False):
        """Alias for after_server_stopped"""
        return self.before_server_start(callback, before_plugins=before_plugins)

    def before_server_start(self, callback=None, *, before_plugins=False):
        return self._make_hook_decorator("before_server_start", before_plugins, callback)

    def after_server_stopped(self, callback=None, *, before_plugins=False):
        return self._make_hook_decorator("after_server_stopped", before_plugins, callback)

    def before_rpc_call(self, callback=None, *, before_plugins=False):
        return self._make_hook_decorator("before_rpc_call", before_plugins, callback)

    def after_rpc_call(self, callback=None, *, before_plugins=False):
        return self._make_hook_decorator("after_rpc_call", before_plugins, callback)

    def before_rpc_execution(self, callback=None, *, before_plugins=False):
        return self._make_hook_decorator("before_rpc_execution", before_plugins, callback)

    def after_rpc_execution(self, callback=None, *, before_plugins=False):
        return self._make_hook_decorator("after_rpc_execution", before_plugins, callback)

    def before_event_sent(self, callback=None, *, before_plugins=False):
        return self._make_hook_decorator("before_event_sent", before_plugins, callback)

    def after_event_sent(self, callback=None, *, before_plugins=False):
        return self._make_hook_decorator("after_event_sent", before_plugins, callback)

    def before_event_execution(self, callback=None, *, before_plugins=False):
        return self._make_hook_decorator("before_event_execution", before_plugins, callback)

    def after_event_execution(self, callback=None, *, before_plugins=False):
        return self._make_hook_decorator("after_event_execution", before_plugins, callback)


class _EventListener(object):
    """ Logic for setting up listener tasks for 1 or more events

    This class will take a list of events and a callable. Background
    tasks will be setup to listen for the specified events
    (see make_task()) and the provided callable will be called when the
    event is detected.

    This logic is smart enough to detect when the APIs of the events use
    the same/different transports. See get_event_transports().

    Note that this class is tightly coupled to BusClient. It has been
    extracted for readability. For this reason is a private class,
    the API of which should not be relied upon externally.
    """

    def __init__(
        self,
        *,
        events: List[Tuple[str, str]],
        listener_callable,
        options: dict = None,
        bus_client: "BusClient",
    ):
        self.events = events
        self.listener_callable = listener_callable
        self.options = options or {}
        self.bus_client = bus_client

        self.options.setdefault("listener_name", "default")

        self.event_transports = self.get_event_transports()

    def get_event_transports(self):
        """ Get events grouped by transport

        It is possible that the specified events will be handled
        by different transports. Therefore group the events
        by transport so we can setup the listeners with the
        appropriate transport for each event.
        """
        return self.bus_client.transport_registry.get_event_transports(
            api_names=[api_name for api_name, _ in self.events]
        )

    @property
    @functools.lru_cache()
    def on_error(self) -> "OnError":
        """What should happen when the listener callable throws an error?

        Will use the most severe form of error handling for all
        transports in use by this listener
        """
        on_error_values = []
        for *_, _api_names in self.event_transports:
            on_error_values.extend(
                [self.bus_client.config.api(_api_name).on_error for _api_name in _api_names]
            )

        if OnError.SHUTDOWN in on_error_values:
            return OnError.SHUTDOWN
        elif OnError.STOP_LISTENER in on_error_values:
            return OnError.STOP_LISTENER
        else:
            return OnError.IGNORE

    @property
    def die_on_error(self) -> bool:
        """Should the entire process die if an error occurs in a listener?"""
        return self.on_error == OnError.SHUTDOWN

    def make_task(self) -> asyncio.Task:
        """ Create a task responsible for running the listener(s)

        This will create a task for each transport (see get_event_transports()).
        These tasks will be gathered together into a parent task, which is then
        returned.

        Any unhandled exceptions will be dealt with according to `on_error`.

        See listener() for the coroutine which handles the listening.
        """
        tasks = []
        for _event_transport, _api_names in self.event_transports:
            # Create a listener task for each event transport,
            # passing each a list of events for which it should listen
            events = [
                (api_name, event_name)
                for api_name, event_name in self.events
                if api_name in _api_names
            ]

            task = asyncio.ensure_future(self.listener(_event_transport, events))
            task.is_listener = True  # Used by close()
            tasks.append(task)

        listener_task = asyncio.gather(*tasks)

        exception_checker = make_exception_checker(die=self.die_on_error)
        listener_task.add_done_callback(exception_checker)

        # Setting is_listener lets Client.close() know that it should mop up this
        # task automatically on shutdown
        listener_task.is_listener = True

        return listener_task

    async def listener(self, event_transport: EventTransport, events: List[Tuple[str, str]]):
        """ Receive events from the transport and invoke the listener callable

        This is the core glue which combines the event transports' consume()
        method and the listener callable. The bulk of this is logging,
        validation, plugin hooks, and error handling.
        """
        # event_transport.consume() returns an asynchronous generator
        # which will provide us with messages
        consumer = event_transport.consume(listen_for=events, **self.options)

        with self.bus_client._register_listener(events):
            async for event_messages in consumer:
                for event_message in event_messages:
                    # TODO: Check events match those requested
                    # TODO: Support event name of '*', but transports should raise
                    # TODO: an exception if it is not supported.
                    logger.info(
                        L(
                            "📩  Received event {}.{} with ID {}".format(
                                Bold(event_message.api_name),
                                Bold(event_message.event_name),
                                event_message.id,
                            )
                        )
                    )

                    self.bus_client._validate(event_message, "incoming")

                    await self.bus_client._plugin_hook(
                        "before_event_execution", event_message=event_message
                    )

                    if self.bus_client.config.api(event_message.api_name).cast_values:
                        parameters = cast_to_signature(
                            parameters=event_message.kwargs, callable=self.listener_callable
                        )
                    else:
                        parameters = event_message.kwargs

                    try:
                        # Call the listener
                        co = self.listener_callable(
                            # Pass the event message as a positional argument,
                            # thereby allowing listeners to have flexibility in the argument names.
                            # (And therefore allowing listeners to use the `event` parameter themselves)
                            event_message,
                            **parameters,
                        )

                        # Support awaitable event listeners
                        if inspect.isawaitable(co):
                            await co

                    except LightbusShutdownInProgress as e:
                        logger.info("Shutdown in progress: {}".format(e))
                    except Exception as e:
                        if self.on_error == OnError.IGNORE:
                            # We're ignore errors, so log it and move on
                            logger.error(
                                f"An event listener raised an exception while processing an event. Lightbus will "
                                f"continue as normal because the on 'on_error' option is set "
                                f"to '{OnError.IGNORE.value}'."
                            )
                        elif self.on_error == OnError.STOP_LISTENER:
                            logger.error(
                                f"An event listener raised an exception while processing an event. Lightbus will "
                                f"stop the listener but keep on running. This is because the 'on_error' option "
                                f"is set to '{OnError.STOP_LISTENER.value}'."
                            )
                            # Stop the listener by returning
                            return
                        else:
                            # We're not ignoring errors, so raise it and
                            # let the error handler callback deal with it
                            raise

                    # Acknowledge the successfully processed message
                    await event_transport.acknowledge(event_message)

                    await self.bus_client._plugin_hook(
                        "after_event_execution", event_message=event_message
                    )
