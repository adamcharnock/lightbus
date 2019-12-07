import asyncio
import functools
import inspect
from typing import List, Tuple, Callable, Optional, Dict

from lightbus import BusClient, EventTransport, Parameter, EventMessage
from lightbus.client.base_subclient import BaseSubClient
from lightbus.client.bus_client import logger
from lightbus.client.utilities import validate_event_or_rpc_name, queue_exception_checker
from lightbus.client.validator import validate_outgoing, validate_incoming
from lightbus.exceptions import (
    UnknownApi,
    EventNotFound,
    InvalidEventArguments,
    InvalidEventListener,
)
from lightbus.log import L, Bold
from lightbus.mediator import commands
from lightbus.mediator.commands import (
    SendEventCommand,
    AcknowledgeEventCommand,
    ConsumeEventsCommand,
)
from lightbus.transports.base import TransportRegistry
from lightbus.utilities.async_tools import run_user_provided_callable
from lightbus.utilities.casting import cast_to_signature
from lightbus.utilities.deforming import deform_to_bus
from lightbus.utilities.singledispatch import singledispatchmethod


class EventBusClient(BaseSubClient):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._event_listeners: Dict[str, EventListener] = {}

    async def fire_event(self, api_name, name, kwargs: dict = None, options: dict = None):
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

        validate_event_or_rpc_name(api_name, "event", name)

        try:
            event = api.get_event(name)
        except EventNotFound:
            raise EventNotFound(
                "Lightbus tried to fire the event {api_name}.{name}, but the API {api_name} does not "
                "seem to contain an event named {name}. You may need to define the event, you "
                "may also be using the incorrect API. Also check for typos.".format(**locals())
            )

        parameter_names = {p.name if isinstance(p, Parameter) else p for p in event.parameters}

        if set(kwargs.keys()) != parameter_names:
            raise InvalidEventArguments(
                "Invalid event arguments supplied when firing event. Attempted to fire event with "
                "{} arguments: {}. Event expected {}: {}".format(
                    len(kwargs),
                    sorted(kwargs.keys()),
                    len(event.parameters),
                    sorted(parameter_names),
                )
            )

        kwargs = deform_to_bus(kwargs)
        event_message = EventMessage(
            api_name=api.meta.name, event_name=name, kwargs=kwargs, version=api.meta.version
        )

        validate_outgoing(self.config, self.schema, event_message)

        await self._execute_hook("before_event_sent", event_message=event_message)
        logger.info(L("📤  Sending event {}.{}".format(Bold(api_name), Bold(name))))

        await self.send_to_event_transports(
            SendEventCommand(message=event_message, options=options)
        ).wait()

        await self._execute_hook("after_event_sent", event_message=event_message)

    async def listen(
        self,
        events: List[Tuple[str, str]],
        listener: Callable,
        listener_name: str,
        options: dict = None,
    ):
        sanity_check_listener(listener)

        for api_name, name in events:
            validate_event_or_rpc_name(api_name, "event", name)

        await self.send_to_event_transports(
            ConsumeEventsCommand(events=events, destination_queue=foo_queue)
        ).wait()

        event_listener = EventListener(
            events=events,
            listener_name=listener_name,
            options=options,
            on_message=functools.partial(self._on_message, listener=listener, options=options),
            fatal_errors=self.fatal_errors,
            transport_registry=self.transport_registry,
        )

        if listener_name in self._event_listeners:
            # TODO: Custom exception class
            raise Exception(f"Listener with name {listener_name} already registered")

        self._event_listeners[listener_name] = event_listener

    async def _on_message(self, event_message: EventMessage, listener: Callable, options: dict):

        # TODO: Check events match those requested
        # TODO: Support event name of '*', but transports should raise
        # TODO: an exception if it is not supported.
        logger.info(
            L(
                "📩  Received event {}.{} with ID {}".format(
                    Bold(event_message.api_name), Bold(event_message.event_name), event_message.id
                )
            )
        )

        validate_incoming(self.config, self.schema, event_message)

        await self.bus_client._execute_hook("before_event_execution", event_message=event_message)

        if self.config.api(event_message.api_name).cast_values:
            parameters = cast_to_signature(parameters=event_message.kwargs, callable=listener)
        else:
            parameters = event_message.kwargs

        # Call the listener.
        # Pass the event message as a positional argument,
        # thereby allowing listeners to have flexibility in the argument names.
        # (And therefore allowing listeners to use the `event` parameter themselves)
        await run_user_provided_callable(
            listener, args=[event_message], kwargs=parameters, error_queue=self.fatal_errors
        )

        # Acknowledge the successfully processed message
        await self.send_to_event_transports(
            AcknowledgeEventCommand(message=event_message, options=options)
        ).wait()

        await self.bus_client._execute_hook("after_event_execution", event_message=event_message)

    @singledispatchmethod
    async def handle(self, command):
        raise NotImplementedError(f"Did not recognise command {command.__name__}")

    @handle.register
    async def handle_receive_event(self, command: commands.ReceiveEventCommand):
        if command.listener_name not in self._event_listeners:
            logger.debug(
                f"Received an event for unknown listener '%s'. Had: %s",
                command.listener_name,
                self._event_listeners.keys(),
            )
            return

        listener = self._event_listeners[command.listener_name]
        await listener.incoming_events.put(command.message)


class EventListener:
    """ Logic for setting up listener tasks for 1 or more events

    This class will take a list of events and a callable. Background
    tasks will be setup to listen for the specified events
    (see start_task()) and the provided callable will be called when the
    event is detected.

    This logic is smart enough to detect when the APIs of the events use
    the same/different transports. See get_event_transports().

    Note that this class is tightly coupled to EventBusClient. It has been
    extracted for readability. For this reason is a private class,
    the API of which should not be relied upon externally.
    """

    def __init__(
        self,
        *,
        events: List[Tuple[str, str]],
        listener_name: str,
        options: dict = None,
        transport_registry: TransportRegistry,
        on_message: Callable,
        fatal_errors: asyncio.Queue,
    ):
        self.events = events
        self.listener_name = listener_name
        self.options = options or {}
        self.transport_registry = transport_registry
        self.on_message = on_message
        self.fatal_errors = fatal_errors
        # TODO: Generalise the queue monitor and apply to this
        # TODO: Replace 10 with the value from the config
        self.incoming_events = asyncio.Queue(maxsize=10)

        self.listener_task: Optional[asyncio.Task] = None

    @property
    @functools.lru_cache()
    def event_transports(self):
        """ Get events grouped by transport

        It is possible that the specified events will be handled
        by different transports. Therefore group the events
        by transport so we can setup the listeners with the
        appropriate transport for each event.
        """
        return self.transport_registry.get_event_transports(
            api_names=[api_name for api_name, _ in self.events]
        )

    def start_task(self, bus_client: BusClient):
        """ Create a task responsible for running the listener(s)

        This will create a task for each transport (see get_event_transports()).
        These tasks will be gathered together into a parent task, which is then
        returned.

        Any unhandled exceptions will be dealt with according to `on_error`.

        See listener() for the coroutine which handles the listening.
        """
        # TODO: Move to routing layer
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

        exception_checker = queue_exception_checker(queue=self.fatal_errors)
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
        # consumer = event_transport.consume(
        #     listen_for=events,
        #     listener_name=self.listener_name,
        #     bus_client=bus_client,
        #     **self.options,
        # )

        while True:
            event_message = await self.incoming_events.get()
            await self.on_message(event_message=event_message)


def sanity_check_listener(listener):
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
