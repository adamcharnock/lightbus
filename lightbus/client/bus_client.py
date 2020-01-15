import asyncio
import logging
from datetime import timedelta
from functools import wraps
from inspect import iscoroutinefunction
from typing import List, Tuple, Coroutine, Union, Sequence, TYPE_CHECKING, Callable

from lightbus.client.utilities import queue_exception_checker, Error, ErrorQueueType, OnError
from lightbus.exceptions import (
    InvalidSchedule,
    BusAlreadyClosed,
    UnsupportedUse,
    AsyncFunctionOrMethodRequired,
)
from lightbus.log import LBullets
from lightbus.utilities.async_tools import (
    block,
    get_event_loop,
    cancel,
    call_every,
    call_on_schedule,
    cancel_and_log_exceptions,
)
from lightbus.utilities.features import Feature, ALL_FEATURES
from lightbus.utilities.frozendict import frozendict

if TYPE_CHECKING:
    # pylint: disable=unused-import,cyclic-import
    from schedule import Job
    from lightbus.config import Config
    from lightbus.client.subclients.event import EventClient
    from lightbus.client.subclients.rpc_result import RpcResultClient
    from lightbus.plugins import PluginRegistry
    from lightbus.schema import Schema
    from lightbus.api import Api, ApiRegistry
    from lightbus.transports.registry import TransportRegistry
    from lightbus.hooks import HookRegistry

__all__ = ["BusClient"]


logger = logging.getLogger(__name__)


def raise_queued_errors(fn):
    """Decorator to raise any errors which have appeared on the bus' error_queue during execution

    There are two situations in which we can categorise errors:

        1. Errors which occur from a user directly accessing the bus.
           For example, calling an RPC or firing an event
        2. Errors which occur within background tasks which are running as
           part of the Lightbus worker

    This decorator handles the former. The BusClient's error_monitor() handles the second
    """
    if not iscoroutinefunction(fn):
        raise AsyncFunctionOrMethodRequired(
            "@raise_queued_errors can only be used on async methods"
        )

    @wraps(fn)
    async def _wrapped(self: "BusClient", *args, **kwargs):
        result = await fn(self, *args, **kwargs)
        await self.raise_errors()
        return result

    return _wrapped


class BusClient:
    """Provides a the lower level interface for accessing the bus

    The low-level `BusClient` is less expressive than the interface provided by `BusPath`,
    but does allow for more control in some situations.

    All functionality in `BusPath` is provided by `BusClient`.
    """

    def __init__(
        self,
        config: "Config",
        schema: "Schema",
        plugin_registry: "PluginRegistry",
        hook_registry: "HookRegistry",
        event_client: "EventClient",
        rpc_result_client: "RpcResultClient",
        api_registry: "ApiRegistry",
        transport_registry: "TransportRegistry",
        error_queue: ErrorQueueType,
        features: Sequence[Union[Feature, str]] = ALL_FEATURES,
    ):
        self.config = config
        self.schema = schema
        self.plugin_registry = plugin_registry
        self.hook_registry = hook_registry
        self.event_client = event_client
        self.rpc_result_client = rpc_result_client
        self.api_registry = api_registry
        # The transport registry isn't actually used by the bus client, but it is
        # useful to have it available as a property on the client.
        self.transport_registry = transport_registry
        self.error_queue = error_queue

        # Coroutines added via schedule/every/add_background_task which should be started up
        # once the worker starts
        self._background_coroutines = []
        # Tasks produced from the values in self._background_coroutines. Will be closed on bus shutdown
        self._background_tasks = []

        self.features: List[Union[Feature, str]] = []
        self.set_features(ALL_FEATURES)

        self.exit_code = 0
        self._closed = False
        self._worker_tasks = set()
        self._lazy_load_complete = False

        # Used to detect if the event monitor is running
        self._error_monitor_lock = asyncio.Lock()

    def close(self):
        """Close the bus client

        This will cancel all tasks and close all transports/connections
        """
        block(self.close_async())

    async def close_async(self):
        """Async version of close()
        """
        if self._closed:
            raise BusAlreadyClosed()

        await cancel_and_log_exceptions(*self._background_tasks)

        await self.event_client.close()
        await self.rpc_result_client.close()
        await self.schema.close()

        while not self.error_queue.empty():
            logger.error(self.error_queue.get_nowait())

        self._closed = True

    @property
    def loop(self):
        return get_event_loop()

    def run_forever(self):
        block(self.start_worker())

        self._actually_run_forever()
        logger.debug("Main thread event loop was stopped")

        # Close down the worker
        logger.debug("Stopping worker")
        block(self.stop_worker())

        # Close down the client
        logger.debug("Closing bus")
        block(self.close_async())

        return self.exit_code

    async def start_worker(self):
        """Worker startup procedure
        """
        # Ensure an event loop exists
        get_event_loop()

        self._worker_tasks = set()

        # Start monitoring for errors on the error queue
        error_monitor_task = asyncio.ensure_future(self.error_monitor())
        self._error_monitor_task = error_monitor_task
        self._worker_tasks.add(self._error_monitor_task)

        # Features setup & logging
        if not self.api_registry.all() and Feature.RPCS in self.features:
            logger.info("Disabling serving of RPCs as no APIs have been registered")
            self.features.remove(Feature.RPCS)

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

        # Api logging
        logger.info(
            LBullets(
                "APIs in registry ({})".format(len(self.api_registry.all())),
                items=self.api_registry.names(),
            )
        )

        # Push all registered APIs into the global schema
        for api in self.api_registry.all():
            await self.schema.add_api(api)

        # We're running as a worker now (e.g. lightbus run), so
        # do the lazy loading immediately
        await self.lazy_load_now()

        # Setup schema monitoring
        monitor_task = asyncio.ensure_future(
            queue_exception_checker(self.schema.monitor(), self.error_queue)
        )

        logger.info("Executing before_worker_start & on_start hooks...")
        await self.hook_registry.execute("before_worker_start")
        logger.info("Execution of before_worker_start & on_start hooks was successful")

        # Setup RPC consumption
        if Feature.RPCS in self.features:
            consume_rpc_task = asyncio.ensure_future(
                queue_exception_checker(self.consume_rpcs(), self.error_queue)
            )
        else:
            consume_rpc_task = None

        # Start off any registered event listeners
        if Feature.EVENTS in self.features:
            await self.event_client.start_registered_listeners()

        # Start off any background tasks
        if Feature.TASKS in self.features:
            for coroutine in self._background_coroutines:
                task = asyncio.ensure_future(queue_exception_checker(coroutine, self.error_queue))
                self._background_tasks.append(task)

        self._worker_tasks.add(consume_rpc_task)
        self._worker_tasks.add(monitor_task)

    async def stop_worker(self):
        logger.debug("Stopping worker")

        # Cancel the tasks we created above
        await cancel(*self._worker_tasks)

        logger.info("Executing after_worker_stopped & on_stop hooks...")
        await self.hook_registry.execute("after_worker_stopped")
        logger.info("Execution of after_worker_stopped & on_stop hooks was successful")

    def _actually_run_forever(self):  # pragma: no cover
        """Simply start the loop running forever

        This just makes testing easier as we can mock out this method
        """
        self.loop.run_forever()

    async def raise_errors(self):
        # If the error monitor is running then just return, as that means we are
        # running as a worker and so can rely on the error monitor to pickup the
        # errors which an happen in the various background tasks
        if self._error_monitor_lock.locked():
            return

        # Check queue for errors
        try:
            error: Error = self.error_queue.get_nowait()
        except asyncio.QueueEmpty:
            # No errors, everything ok
            return
        else:
            # If there is an error then raise it
            raise error.value

    async def error_monitor(self):
        async with self._error_monitor_lock:
            error: Error = await self.error_queue.get()
            logger.debug(f"Bus client event monitor detected an error, will shutdown.")

            if error.should_show_error():
                logger.error(str(error))
                if error.help:
                    logger.error(error.help)
            else:
                logger.info(error.value)

            self.exit_code = error.exit_code
            self.stop_loop()

    def stop_loop(self):
        self.loop.stop()

    def request_shutdown(self):
        logger.debug("Requesting Lightbus shutdown")
        self.error_queue.put_nowait(
            Error(
                type=KeyboardInterrupt,
                value=KeyboardInterrupt("Shutdown requested"),
                traceback=None,
                invoking_stack=None,
                exit_code=0,
            )
        )

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

        # 2. Ensure the schema transport is loaded (other transports will be
        #    loaded as the need arises, but we need schema information from the get-go)
        await self.schema.ensure_loaded_from_bus()

        # 3. Add any local APIs to the schema
        for api in self.api_registry.all():
            await self.schema.add_api(api)

        logger.info(
            LBullets(
                "Loaded the following local schemas ({})".format(len(self.schema.remote_schemas)),
                items=self.schema.local_schemas.keys(),
            )
        )

        # 4. Done
        self._lazy_load_complete = True

    # RPCs

    @raise_queued_errors
    async def consume_rpcs(self, apis: List["Api"] = None):
        """Start a background task to consume RPCs

        This will consumer RPCs on APIs which have been registered with this
        bus client.
        """
        return await self.rpc_result_client.consume_rpcs(apis)

    @raise_queued_errors
    async def call_rpc_remote(
        self, api_name: str, name: str, kwargs: dict = frozendict(), options: dict = frozendict()
    ):
        """ Perform an RPC call

        Call an RPC and return the result.
        """
        await self.lazy_load_now()
        return await self.rpc_result_client.call_rpc_remote(
            api_name=api_name, name=name, kwargs=kwargs, options=options
        )

    # Events

    @raise_queued_errors
    async def fire_event(self, api_name, name, kwargs: dict = None, options: dict = None):
        """Fire an event onto the bus"""
        await self.lazy_load_now()
        return await self.event_client.fire_event(
            api_name=api_name, name=name, kwargs=kwargs, options=options
        )

    def listen_for_event(
        self,
        api_name: str,
        name: str,
        listener: Callable,
        listener_name: str,
        options: dict = None,
        on_error: OnError = OnError.SHUTDOWN,
    ):
        """Listen for a single event

        Wraps `listen_for_events()`
        """
        self.listen_for_events(
            [(api_name, name)], listener, listener_name=listener_name, options=options or {}
        )

    def listen_for_events(
        self,
        events: List[Tuple[str, str]],
        listener: Callable,
        listener_name: str,
        options: dict = None,
        on_error: OnError = OnError.SHUTDOWN,
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
        return self.event_client.listen(
            events=events,
            listener=listener,
            listener_name=listener_name,
            options=options,
            on_error=on_error,
        )

    def add_background_task(self, coroutine: Union[Coroutine, asyncio.Future]):
        """Run a coroutine in the background

        The provided coroutine will be run in the background once
        Lightbus startup is complete.

        The coroutine will be cancelled when the bus client is closed.

        The Lightbus process will exit if the coroutine raises an exception.
        See lightbus.utilities.async_tools.check_for_exception() for details.
        """

        # Store coroutine for starting once the worker starts
        self._background_coroutines.append(coroutine)

    # Utilities

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

    def _make_hook_decorator(self, name, before_plugins=False, callback=None):
        if callback and not callable(callback):
            raise AssertionError("The provided callback is not callable")

        if callback:
            self.hook_registry.register_callback(name, callback, before_plugins)
            return None
        else:

            def hook_decorator(fn):
                self.hook_registry.register_callback(name, fn, before_plugins)
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
                callback=f, timedelta=td, also_run_immediately=also_run_immediately
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
                callback=f, schedule=schedule, also_run_immediately=also_run_immediately
            )
            self.add_background_task(coroutine)
            return f

        return wrapper

    # API registration

    def register_api(self, api: "Api"):
        """Register an API with this bus client

        You must register APIs which you wish to fire events
        on or handle RPCs calls for.

        See Also: https://lightbus.org/explanation/apis/
        """
        self.api_registry.add(api)
