import asyncio
import functools
import inspect
import logging
import time
from collections import defaultdict
from datetime import timedelta
from itertools import chain
from typing import List, Tuple, Coroutine, Union, Sequence, TYPE_CHECKING, Callable

import janus

from lightbus.api import Api, ApiRegistry
from lightbus.client_worker import (
    ClientWorker,
    run_in_worker_thread,
    assert_not_in_worker_thread,
    WorkerProxy,
)
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
    InvalidSchedule,
    BusAlreadyClosed,
    TransportIsClosed,
    UnsupportedUse,
)
from lightbus.internal_apis import LightbusStateApi, LightbusMetricsApi
from lightbus.log import LBullets, L, Bold
from lightbus.message import RpcMessage, ResultMessage, EventMessage, Message
from lightbus.plugins import PluginRegistry
from lightbus.schema import Schema
from lightbus.schema.schema import _parameter_names
from lightbus.transports import RpcTransport
from lightbus.transports.base import TransportRegistry, EventTransport
from lightbus.utilities.async_tools import (
    block,
    get_event_loop,
    cancel,
    make_exception_checker,
    call_every,
    call_on_schedule,
    run_user_provided_callable,
)
from lightbus.utilities.casting import cast_to_signature
from lightbus.utilities.deforming import deform_to_bus
from lightbus.utilities.features import Feature, ALL_FEATURES
from lightbus.utilities.frozendict import frozendict
from lightbus.utilities.human import human_time
from lightbus.utilities.logging import log_transport_information

if TYPE_CHECKING:
    # pylint: disable=unused-import,cyclic-import
    from schedule import Job
    from lightbus.config import Config

__all__ = ["BusClient"]


logger = logging.getLogger(__name__)


class BusClient:
    """Provides a the lower level interface for accessing the bus

    The low-level `BusClient` is less expressive than the interface provided by `BusPath`,
    but does allow for more control in some situations.

    All functionality in `BusPath` is provided by `BusClient`.
    """

    def __init__(
        self,
        config: "Config",
        transport_registry: TransportRegistry = None,
        features: Sequence[Union[Feature, str]] = ALL_FEATURES,
        plugins=None,
    ):
        self._event_listeners: List[_EventListener] = []  # Event listeners
        self._consumers = []  # RPC consumers
        # Coroutines added via schedule/every/add_background_task which should be started up
        # once the server starts
        self._background_coroutines = []
        # Tasks produced from the values in self._background_coroutines. Will be closed on bus shutdown
        self._background_tasks = []
        self._hook_callbacks = defaultdict(list)
        self.config = config
        self.transport_registry = transport_registry
        self.features: List[Union[Feature, str]] = ALL_FEATURES
        self.set_features(list(features))
        self.api_registry = ApiRegistry()
        self.plugin_registry = PluginRegistry()
        self.schema = None
        self._server_shutdown_queue: janus.Queue = None
        self._shutdown_monitor_task = None
        self.exit_code = 0
        self._closed = False
        self._server_tasks = []
        self.worker = ClientWorker()
        self._lazy_load_complete = False

        if plugins is None:
            logger.debug("Auto-loading any installed Lightbus plugins...")
            self.plugin_registry.autoload_plugins(self.config)
        else:
            logger.debug("Loading explicitly specified Lightbus plugins....")
            self.plugin_registry.set_plugins(plugins)

        self.worker.start(bus_client=self, after_shutdown=self._handle_worker_shutdown)
        self.__init_worker___()

    @run_in_worker_thread()
    def __init_worker___(self):
        self.transport_registry = self.transport_registry or TransportRegistry().load_config(
            self.config
        )
        schema = Schema(
            schema_transport=self.transport_registry.get_schema_transport(),
            max_age_seconds=self.config.bus().schema.ttl,
            human_readable=self.config.bus().schema.human_readable,
        )
        self.schema = WorkerProxy(proxied=schema, worker=self.worker)

    def _handle_worker_shutdown(self):
        """ Cleanup the BusClient due to worker thread shutdown

        This method will be called within the worker thead,
        but after the worker thread's event loop has stopped.
        """

        try:
            # Close _close_async_inner() because, we are in the worker thead.
            block(self._close_async_inner())
        except BusAlreadyClosed:
            # In the case of a clean shutdown the bus will already be closed.
            pass

    def welcome_message(self, plugins: list = None):
        """Show the server-startup welcome message
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

        if self.plugin_registry._plugins:
            logger.info(
                LBullets(
                    "Loaded the following plugins ({})".format(len(self.plugin_registry._plugins)),
                    items=self.plugin_registry._plugins,
                )
            )
        else:
            logger.info("No plugins loaded")

    @assert_not_in_worker_thread()
    def close(self, _stop_worker=True):
        """Close the bus client

        This will cancel all tasks and close all transports/connections
        """
        if self._closed:
            raise BusAlreadyClosed()
        block(self.close_async(_stop_worker=_stop_worker))

    @assert_not_in_worker_thread()
    async def close_async(self, _stop_worker=True):
        """Async version of close()
        """
        try:
            if self._closed:
                raise BusAlreadyClosed()

            await self._close_async_inner()
        finally:
            # Whatever happens, make sure we stop the event loop otherwise the
            # bus thread will keep running and prevent the process for exiting
            if _stop_worker:
                self.worker.shutdown()
                await self.worker.wait_for_shutdown()

    @run_in_worker_thread()
    async def _close_async_inner(self):
        """Handle all aspects of the closing which need to run within the bus worker thread"""
        if self._closed:
            raise BusAlreadyClosed()

        listener_tasks = [
            task for task in asyncio.all_tasks() if getattr(task, "is_listener", False)
        ]

        for task in chain(listener_tasks, self._background_tasks):
            # pylint: disable=broad-except
            try:
                await cancel(task)
            except Exception as e:
                logger.exception(e)

        for transport in self.transport_registry.get_all_transports():
            await transport.close()

        await self.schema.schema_transport.close()

        self._closed = True

    @property
    def loop(self):
        return get_event_loop()

    def run_forever(self):
        if not self.api_registry.all() and Feature.RPCS in self.features:
            logger.info("Disabling serving of RPCs as no APIs have been registered")
            self.features.remove(Feature.RPCS)

        self.start_server()

        self._actually_run_forever()
        logger.debug("Main thread event loop was stopped")

        # Stopping the server requires access to the worker,
        # so do this first
        logger.debug("Stopping server")
        self.stop_server()

        # Here we close connections and shutdown the worker thread
        logger.debug("Closing bus")
        self.close()

    def shutdown_server(self, exit_code):
        if self._server_shutdown_queue is not None:
            # If this shutdown queue *is* None, then it is safe to assume
            # the server hasn't started up yet, so no need to
            # put anything in the shutdown queue
            self._server_shutdown_queue.sync_q.put(exit_code)

    @assert_not_in_worker_thread()
    def start_server(self):
        """Server startup procedure

        Must be called from within the main thread. Handles the niceties around
        starting and stopping the server. The interesting setup happens in
        BusClient._setup_server()
        """
        self.welcome_message()

        # Ensure an event loop exists
        get_event_loop()

        self._server_shutdown_queue = janus.Queue()
        self._server_tasks = set()

        async def server_shutdown_monitor():
            exit_code = await self._server_shutdown_queue.async_q.get()
            self.exit_code = exit_code
            self.loop.stop()
            self._server_shutdown_queue.async_q.task_done()

        shutdown_monitor_task = asyncio.ensure_future(server_shutdown_monitor())
        shutdown_monitor_task.add_done_callback(make_exception_checker(self, die=True))
        self._shutdown_monitor_task = shutdown_monitor_task

        logger.info(
            LBullets(
                f"Enabled features ({len(self.features)})", items=[f.value for f in self.features]
            )
        )

        disabled_features = set(ALL_FEATURES) - set(self.features)
        logger.info(
            LBullets(
                f"Disabled features ({len(disabled_features)})",
                items=[f.value for f in disabled_features],
            )
        )

        block(self._setup_server())

    @run_in_worker_thread()
    async def _setup_server(self):
        self.api_registry.add(LightbusStateApi())
        self.api_registry.add(LightbusMetricsApi())

        logger.info(
            LBullets(
                "APIs in registry ({})".format(len(self.api_registry.all())),
                items=self.api_registry.names(),
            )
        )

        # Push all registered APIs into the global schema
        for api in self.api_registry.all():
            await self.schema.add_api(api)

        # We're running as a server now (e.g. lightbus run), so
        # do the lazy loading immediately
        await self.lazy_load_now()

        # Setup schema monitoring
        monitor_task = asyncio.ensure_future(self.schema.monitor())
        monitor_task.add_done_callback(make_exception_checker(self, die=True))

        logger.info("Executing before_worker_start & on_start hooks...")
        await self._execute_hook("before_worker_start")
        logger.info("Execution of before_worker_start & on_start hooks was successful")

        # Setup RPC consumption
        if Feature.RPCS in self.features:
            consume_rpc_task = asyncio.ensure_future(self.consume_rpcs())
            consume_rpc_task.add_done_callback(make_exception_checker(self, die=True))
        else:
            consume_rpc_task = None

        # Start off any registered event listeners
        if Feature.EVENTS in self.features:
            for event_listener in self._event_listeners:
                event_listener.start_task(bus_client=self)

        # Start off any background tasks
        if Feature.TASKS in self.features:
            for coroutine in self._background_coroutines:
                task = asyncio.ensure_future(coroutine)
                task.add_done_callback(make_exception_checker(self, die=True))
                self._background_tasks.append(task)

        self._server_tasks = [consume_rpc_task, monitor_task]

    @assert_not_in_worker_thread()
    def stop_server(self):
        block(cancel(self._shutdown_monitor_task))
        block(self._stop_server_inner())

    @run_in_worker_thread()
    async def _stop_server_inner(self):
        # Cancel the tasks we created above
        await cancel(*self._server_tasks)

        logger.info("Executing after_worker_stopped & on_stop hooks...")
        await self._execute_hook("after_worker_stopped")
        logger.info("Execution of after_worker_stopped & on_stop hooks was successful")

    def _actually_run_forever(self):  # pragma: no cover
        """Simply start the loop running forever

        This just makes testing easier as we can mock out this method
        """
        self.loop.run_forever()

    @run_in_worker_thread()
    async def lazy_load_now(self):
        """Perform lazy tasks immediately

        When lightbus is used as a client it performs network tasks
        lazily. This speeds up import of your bus module, and prevents
        getting surprising errors at import time.

        However, in some cases you may wish to hurry up these lazy tasks
        (or perform them at a known point). In which case you can call this
        method to execute them immediately.
        """
        if self._lazy_load_complete:
            return

        # 1. Load the schema
        logger.debug("Loading schema...")
        await self.schema.ensure_loaded_from_bus()

        logger.info(
            LBullets(
                "Loaded the following remote schemas ({})".format(len(self.schema.remote_schemas)),
                items=self.schema.remote_schemas.keys(),
            )
        )

        # 2. Add any local APIs to the schema
        for api in self.api_registry.all():
            await self.schema.add_api(api)

        logger.info(
            LBullets(
                "Loaded the following local schemas ({})".format(len(self.schema.remote_schemas)),
                items=self.schema.local_schemas.keys(),
            )
        )

        # 3. Open the transports
        for transport in self.transport_registry.get_all_transports():
            await transport.open()

        # 4. Done
        self._lazy_load_complete = True

    # RPCs

    @run_in_worker_thread()
    async def consume_rpcs(self, apis: List[Api] = None):
        """Start a background task to consume RPCs

        This will consumer RPCs on APIs which have been registered with this
        bus client.
        """
        await self.lazy_load_now()

        if apis is None:
            apis = self.api_registry.all()

        if not apis:
            raise NoApisToListenOn(
                "No APIs to consume on in consume_rpcs(). Either this method was called with apis=[], "
                "or the API registry is empty."
            )

        # Not all APIs will necessarily be served by the same transport, so group them
        # accordingly
        api_names = [api.meta.name for api in apis]
        api_names_by_transport = self.transport_registry.get_rpc_transports(api_names)

        coroutines = []
        for rpc_transport, transport_api_names in api_names_by_transport:
            transport_apis = list(map(self.api_registry.get, transport_api_names))
            coroutines.append(
                self._consume_rpcs_with_transport(rpc_transport=rpc_transport, apis=transport_apis)
            )

        task = asyncio.ensure_future(asyncio.gather(*coroutines))
        task.add_done_callback(make_exception_checker(self, die=True))
        self._consumers.append(task)

    async def _consume_rpcs_with_transport(
        self, rpc_transport: RpcTransport, apis: List[Api] = None
    ):
        await self.lazy_load_now()

        while True:
            try:
                rpc_messages = await rpc_transport.consume_rpcs(apis, bus_client=self)
            except TransportIsClosed:
                return

            for rpc_message in rpc_messages:
                self._validate(rpc_message, "incoming")

                await self._execute_hook("before_rpc_execution", rpc_message=rpc_message)
                try:
                    result = await self._call_rpc_local(
                        api_name=rpc_message.api_name,
                        name=rpc_message.procedure_name,
                        kwargs=rpc_message.kwargs,
                    )
                except SuddenDeathException:
                    # Used to simulate message failure for testing
                    return
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    result = e
                else:
                    result = deform_to_bus(result)

                result_message = ResultMessage(result=result, rpc_message_id=rpc_message.id)
                await self._execute_hook(
                    "after_rpc_execution", rpc_message=rpc_message, result_message=result_message
                )

                if not result_message.error:
                    self._validate(
                        result_message,
                        "outgoing",
                        api_name=rpc_message.api_name,
                        procedure_name=rpc_message.procedure_name,
                    )

                await self.send_result(rpc_message=rpc_message, result_message=result_message)

    @run_in_worker_thread()
    async def call_rpc_remote(
        self, api_name: str, name: str, kwargs: dict = frozendict(), options: dict = frozendict()
    ):
        """ Perform an RPC call

        Call an RPC and return the result.
        """
        await self.lazy_load_now()

        rpc_transport = self.transport_registry.get_rpc_transport(api_name)
        result_transport = self.transport_registry.get_result_transport(api_name)

        kwargs = deform_to_bus(kwargs)
        rpc_message = RpcMessage(api_name=api_name, procedure_name=name, kwargs=kwargs)
        return_path = result_transport.get_return_path(rpc_message)
        rpc_message.return_path = return_path
        options = options or {}
        timeout = options.get("timeout", self.config.api(api_name).rpc_timeout)
        # TODO: rpc_timeout is in three different places in the config!
        #       Fix this. Really it makes most sense for the use if it goes on the
        #       ApiConfig rather than having to repeat it on both the result & RPC
        #       transports.
        self._validate_name(api_name, "rpc", name)

        logger.info("📞  Calling remote RPC {}.{}".format(Bold(api_name), Bold(name)))

        start_time = time.time()
        # TODO: It is possible that the RPC will be called before we start waiting for the
        #       response. This is bad.

        self._validate(rpc_message, "outgoing")

        future = asyncio.gather(
            self.receive_result(rpc_message, return_path, options=options),
            rpc_transport.call_rpc(rpc_message, options=options, bus_client=self),
        )

        await self._execute_hook("before_rpc_call", rpc_message=rpc_message)

        try:
            result_message, _ = await asyncio.wait_for(future, timeout=timeout)
            future.result()
        except asyncio.TimeoutError:
            # Allow the future to finish, as per https://bugs.python.org/issue29432
            try:
                await future
                future.result()
            except asyncio.CancelledError:
                pass

            # TODO: Remove RPC from queue. Perhaps add a RpcBackend.cancel() method. Optional,
            #       as not all backends will support it. No point processing calls which have timed out.
            raise LightbusTimeout(
                f"Timeout when calling RPC {rpc_message.canonical_name} after {timeout} seconds. "
                f"It is possible no Lightbus process is serving this API, or perhaps it is taking "
                f"too long to process the request. In which case consider raising the 'rpc_timeout' "
                f"config option."
            ) from None

        await self._execute_hook(
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

    @run_in_worker_thread()
    async def _call_rpc_local(self, api_name: str, name: str, kwargs: dict = frozendict()):
        await self.lazy_load_now()

        api = self.api_registry.get(api_name)
        self._validate_name(api_name, "rpc", name)

        start_time = time.time()
        try:
            method = getattr(api, name)
            if self.config.api(api_name).cast_values:
                kwargs = cast_to_signature(kwargs, method)
            result = await run_user_provided_callable(
                method, args=[], kwargs=kwargs, bus_client=self
            )
        except (asyncio.CancelledError, SuddenDeathException):
            raise
        except Exception as e:
            logging.exception(e)
            logger.warning(
                L(
                    "⚡  Error while executing {}.{}. Took {}",
                    Bold(api_name),
                    Bold(name),
                    human_time(time.time() - start_time),
                )
            )
            raise
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

    @run_in_worker_thread()
    async def fire_event(self, api_name, name, kwargs: dict = None, options: dict = None):
        """Fire an event onto the bus"""
        await self.lazy_load_now()

        kwargs = kwargs or {}
        try:
            api = self.api_registry.get(api_name)
        except UnknownApi:
            raise UnknownApi(
                "Lightbus tried to fire the event {api_name}.{name}, but no API named {api_name} was found in the "
                "registry. An API being in the registry implies you are an authority on that API. Therefore, "
                "Lightbus requires the API to be in the registry as it is a bad idea to fire "
                "events on behalf of remote APIs. However, this could also be caused by a typo in the "
                "API name or event name, or be because the API class has not been "
                "registered using bus.client.register_api(). ".format(**locals())
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
        event_message = EventMessage(
            api_name=api.meta.name, event_name=name, kwargs=kwargs, version=api.meta.version
        )

        self._validate(event_message, "outgoing")

        event_transport = self.transport_registry.get_event_transport(api_name)
        await self._execute_hook("before_event_sent", event_message=event_message)
        logger.info(L("📤  Sending event {}.{}".format(Bold(api_name), Bold(name))))
        await event_transport.send_event(event_message, options=options, bus_client=self)
        await self._execute_hook("after_event_sent", event_message=event_message)

    def listen_for_event(
        self, api_name: str, name: str, listener: Callable, listener_name: str, options: dict = None
    ):
        """Listen for a single event

        Wraps `listen_for_events()`
        """
        self.listen_for_events(
            [(api_name, name)], listener, listener_name=listener_name, options=options
        )

    def listen_for_events(
        self,
        events: List[Tuple[str, str]],
        listener: Callable,
        listener_name: str,
        options: dict = None,
    ):
        """Listen for a list of events

        `events` is in the form:

            events=[
                ('company.first_api', 'event_name'),
                ('company.second_api', 'event_name'),
            ]

        `listener_name` is an arbitrary string which uniquely identifies this listener.
        This can generally be the same as the function name of the `listener` callable, but
        it should not change once deployed.
        """
        self._sanity_check_listener(listener)

        for api_name, name in events:
            self._validate_name(api_name, "event", name)

        event_listener = _EventListener(
            events=events,
            listener_callable=listener,
            listener_name=listener_name,
            options=options,
            bus_client=self,
        )

        # We will start up the event listeners (via start_task())
        # when the server starts. For now we just store them away
        self._event_listeners.append(event_listener)

    # Results

    @run_in_worker_thread()
    async def send_result(self, rpc_message: RpcMessage, result_message: ResultMessage):
        await self.lazy_load_now()
        result_transport = self.transport_registry.get_result_transport(rpc_message.api_name)
        return await result_transport.send_result(
            rpc_message, result_message, rpc_message.return_path, bus_client=self
        )

    @run_in_worker_thread()
    async def receive_result(self, rpc_message: RpcMessage, return_path: str, options: dict):
        await self.lazy_load_now()
        result_transport = self.transport_registry.get_result_transport(rpc_message.api_name)
        return await result_transport.receive_result(
            rpc_message, return_path, options, bus_client=self
        )

    def _validate(self, message: Message, direction: str, api_name=None, procedure_name=None):
        if direction not in ("incoming", "outgoing"):
            raise AssertionError("Invalid direction specified")

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
                    f"Validation is enabled for API named '{api_name}', but there is no schema present for this API. "
                    f"Validation is therefore not possible. You are also seeing this error because the "
                    f"'strict_validation' setting is enabled. Disabling this setting will turn this exception "
                    f"into a warning. "
                )
            else:
                logger.warning(
                    f"Validation is enabled for API named '{api_name}', but there is no schema present for this API. "
                    f"Validation is therefore not possible. You can force this to be an error by enabling "
                    f"the 'strict_validation' config option. You can silence this message by disabling validation "
                    f"for this API using the 'validate' option."
                )
                return

        if isinstance(message, (RpcMessage, EventMessage)):
            self.schema.validate_parameters(api_name, event_or_rpc_name, message.kwargs)
        elif isinstance(message, ResultMessage):
            self.schema.validate_response(api_name, event_or_rpc_name, message.result)

    def add_background_task(self, coroutine: Union[Coroutine, asyncio.Future]):
        """Run a coroutine in the background

        The provided coroutine will be run in the background once
        Lightbus startup is complete.

        The coroutine will be cancelled when the bus client is closed.

        The Lightbus process will exit if the coroutine raises an exception.
        See lightbus.utilities.async_tools.check_for_exception() for details.
        """

        # Store coroutine for starting once the server starts
        self._background_coroutines.append(coroutine)

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

    def set_features(self, features: List[Union[Feature, str]]):
        """Set the features this bus clients should serve.

        Features should be a list of: `rpcs`, `events`, `tasks`
        """
        features = list(features)
        for i, feature in enumerate(features):
            try:
                features[i] = Feature(feature)
            except ValueError:
                features_str = ", ".join([f.value for f in Feature])
                raise UnsupportedUse(f"Feature '{feature}' is not one of: {features_str}\n")

        self.features = features

    # Hooks

    async def _execute_hook(self, name, **kwargs):
        # Hooks that need to run before plugins
        for callback in self._hook_callbacks[(name, True)]:
            await run_user_provided_callable(
                callback, args=[], kwargs=dict(client=self, **kwargs), bus_client=self
            )

        await self.plugin_registry.execute_hook(name, client=self, **kwargs)

        # Hooks that need to run after plugins
        for callback in self._hook_callbacks[(name, False)]:
            await run_user_provided_callable(
                callback, args=[], kwargs=dict(client=self, **kwargs), bus_client=self
            )

    def _register_hook_callback(self, name, fn, before_plugins=False):
        self._hook_callbacks[(name, bool(before_plugins))].append(fn)

    def _make_hook_decorator(self, name, before_plugins=False, callback=None):
        if callback and not callable(callback):
            raise AssertionError("The provided callback is not callable")
        if callback:
            self._register_hook_callback(name, callback, before_plugins)
            return None
        else:

            def hook_decorator(fn):
                self._register_hook_callback(name, fn, before_plugins)
                return fn

            return hook_decorator

    def on_start(self, callback=None, *, before_plugins=False):
        """Decorator to register a function to be called before the worker starts up

        Callback will be called with the following arguments:

            callback(self, *, client: "BusClient")
        """
        return self.before_worker_start(callback, before_plugins=before_plugins)

    def on_stop(self, callback=None, *, before_plugins=False):
        """Decorator to register a function to be called after the worker stops

        Callback will be called with the following arguments:

            callback(self, *, client: "BusClient")
        """
        return self.before_worker_start(callback, before_plugins=before_plugins)

    def before_worker_start(self, callback=None, *, before_plugins=False):
        """Decorator to register a function to be called before the worker starts up

        See `on_start()`
        """
        return self._make_hook_decorator("before_worker_start", before_plugins, callback)

    def after_worker_stopped(self, callback=None, *, before_plugins=False):
        """Decorator to register a function to be called after the worker stops

        See `on_stop()`
        """
        return self._make_hook_decorator("after_worker_stopped", before_plugins, callback)

    def before_rpc_call(self, callback=None, *, before_plugins=False):
        """Decorator to register a function to be called prior to an RPC call

        Callback will be called with the following arguments:

            callback(self, *, rpc_message: RpcMessage, client: "BusClient")
        """
        return self._make_hook_decorator("before_rpc_call", before_plugins, callback)

    def after_rpc_call(self, callback=None, *, before_plugins=False):
        """Decorator to register a function to be called after an RPC call

        Callback will be called with the following arguments:

            callback(self, *, rpc_message: RpcMessage, result_message: ResultMessage, client: "BusClient")
        """
        return self._make_hook_decorator("after_rpc_call", before_plugins, callback)

    def before_rpc_execution(self, callback=None, *, before_plugins=False):
        """Decorator to register a function to be called prior to a local RPC execution

        Callback will be called with the following arguments:

            callback(self, *, rpc_message: RpcMessage, client: "BusClient")
        """
        return self._make_hook_decorator("before_rpc_execution", before_plugins, callback)

    def after_rpc_execution(self, callback=None, *, before_plugins=False):
        """Decorator to register a function to be called after a local RPC execution

        Callback will be called with the following arguments:

            callback(self, *, rpc_message: RpcMessage, result_message: ResultMessage, client: "BusClient")
        """
        return self._make_hook_decorator("after_rpc_execution", before_plugins, callback)

    def before_event_sent(self, callback=None, *, before_plugins=False):
        """Decorator to register a function to be called prior to an event being sent

        Callback will be called with the following arguments:

            callback(self, *, event_message: EventMessage, client: "BusClient")
        """
        return self._make_hook_decorator("before_event_sent", before_plugins, callback)

    def after_event_sent(self, callback=None, *, before_plugins=False):
        """Decorator to register a function to be called after an event was sent

        Callback will be called with the following arguments:

            callback(self, *, event_message: EventMessage, client: "BusClient")
        """
        return self._make_hook_decorator("after_event_sent", before_plugins, callback)

    def before_event_execution(self, callback=None, *, before_plugins=False):
        """Decorator to register a function to be called prior to a local event handler execution

        Callback will be called with the following arguments:

            callback(self, *, event_message: EventMessage, client: "BusClient")
        """
        return self._make_hook_decorator("before_event_execution", before_plugins, callback)

    def after_event_execution(self, callback=None, *, before_plugins=False):
        """Decorator to register a function to be called after a local event handler execution

        Callback will be called with the following arguments:

            callback(self, *, event_message: EventMessage, client: "BusClient")
        """
        return self._make_hook_decorator("after_event_execution", before_plugins, callback)

    # Scheduling

    def every(
        self,
        *,
        seconds=0,
        minutes=0,
        hours=0,
        days=0,
        also_run_immediately=False,
        **timedelta_extra,
    ):
        """ Call a coroutine at the specified interval

        This is a simple scheduling mechanism which you can use in your bus module to setup
        recurring tasks. For example:

            bus = lightbus.create()

            @bus.client.every(seconds=30)
            def my_func():
                print("Hello")

        This can also be used to decorate async functions. In this case the function will be awaited.

        Note that the timing is best effort and is not guaranteed. That being said, execution
        time is accounted for.

        See Also:

            @bus.client.schedule()
        """
        td = timedelta(seconds=seconds, minutes=minutes, hours=hours, days=days, **timedelta_extra)

        if td.total_seconds() == 0:
            raise InvalidSchedule(
                "The @bus.client.every() decorator must be provided with a non-zero time argument. "
                "Ensure you are passing at least one time argument, and that it has a non-zero value."
            )

        # TODO: There is an argument that the backgrounding of this should be done only after
        #       on_start() has been fired. Otherwise this will be run before the on_start() setup
        #       has happened in cases where also_run_immediately=True.
        def wrapper(f):
            coroutine = call_every(  # pylint: assignment-from-no-return
                callback=f, timedelta=td, also_run_immediately=also_run_immediately, bus_client=self
            )
            self.add_background_task(coroutine)
            return f

        return wrapper

    def schedule(self, schedule: "Job", also_run_immediately=False):
        """ Call a coroutine on the specified schedule

        Schedule a task using the `schedule` library:

            import lightbus
            import schedule

            bus = lightbus.create()

            # Run the task every 1-3 seconds, varying randomly
            @bus.client.schedule(schedule.every(1).to(3).seconds)
            def do_it():
                print("Hello using schedule library")

        This can also be used to decorate async functions. In this case the function will be awaited.

        See Also:

            @bus.client.every()
        """

        def wrapper(f):
            coroutine = call_on_schedule(
                callback=f,
                schedule=schedule,
                also_run_immediately=also_run_immediately,
                bus_client=self,
            )
            self.add_background_task(coroutine)
            return f

        return wrapper

    # API registration

    def register_api(self, api: Api):
        """Register an API with this bus client

        You must register APIs which you wish this server to fire events
        on or handle RPCs calls for.

        See Also: https://lightbus.org/explanation/apis/
        """
        self.api_registry.add(api)


class _EventListener:
    """ Logic for setting up listener tasks for 1 or more events

    This class will take a list of events and a callable. Background
    tasks will be setup to listen for the specified events
    (see start_task()) and the provided callable will be called when the
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
        listener_name: str,
        options: dict = None,
        bus_client: "BusClient",
    ):
        self.events = events
        self.listener_callable = listener_callable
        self.listener_name = listener_name
        self.options = options or {}
        self.bus_client = bus_client
        self.listener_task: asyncio.Task = None

    @property
    @functools.lru_cache()
    def event_transports(self):
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

    def start_task(self, bus_client: BusClient) -> asyncio.Task:
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

            task = asyncio.ensure_future(self.listener(_event_transport, events, bus_client))
            task.is_listener = True  # Used by close()
            tasks.append(task)

        listener_task = asyncio.gather(*tasks)

        exception_checker = make_exception_checker(self.bus_client, die=self.die_on_error)
        listener_task.add_done_callback(exception_checker)

        # Setting is_listener lets Client.close() know that it should mop up this
        # task automatically on shutdown
        listener_task.is_listener = True

        self.listener_task = listener_task

    async def listener(
        self, event_transport: EventTransport, events: List[Tuple[str, str]], bus_client: BusClient
    ):
        """ Receive events from the transport and invoke the listener callable

        This is the core glue which combines the event transports' consume()
        method and the listener callable. The bulk of this is logging,
        validation, plugin hooks, and error handling.
        """
        # event_transport.consume() returns an asynchronous generator
        # which will provide us with messages
        consumer = event_transport.consume(
            listen_for=events,
            listener_name=self.listener_name,
            bus_client=bus_client,
            **self.options,
        )

        try:
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

                    await self.bus_client._execute_hook(
                        "before_event_execution", event_message=event_message
                    )

                    if self.bus_client.config.api(event_message.api_name).cast_values:
                        parameters = cast_to_signature(
                            parameters=event_message.kwargs, callable=self.listener_callable
                        )
                    else:
                        parameters = event_message.kwargs

                    try:
                        # Call the listener.
                        # Pass the event message as a positional argument,
                        # thereby allowing listeners to have flexibility in the argument names.
                        # (And therefore allowing listeners to use the `event` parameter themselves)
                        await run_user_provided_callable(
                            self.listener_callable,
                            args=[event_message],
                            kwargs=parameters,
                            bus_client=self.bus_client,
                            die_on_exception=False,
                        )
                    except LightbusShutdownInProgress as e:
                        logger.info("Shutdown in progress: {}".format(e))
                        return
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
                    await event_transport.acknowledge(event_message, bus_client=self.bus_client)

                    await self.bus_client._execute_hook(
                        "after_event_execution", event_message=event_message
                    )
        except asyncio.CancelledError:
            # Close the consumer to allow it to do any cleanup
            try:
                await consumer.aclose()
            except StopAsyncIteration:
                pass
