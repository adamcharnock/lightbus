import asyncio
import inspect
import logging
from typing import List, Tuple, Callable, NamedTuple

from lightbus.schema.schema import Parameter
from lightbus.message import EventMessage
from lightbus.client.subclients.base import BaseSubClient
from lightbus.client.utilities import validate_event_or_rpc_name, queue_exception_checker, OnError
from lightbus.client.validator import validate_outgoing, validate_incoming
from lightbus.exceptions import (
    UnknownApi,
    EventNotFound,
    InvalidEventArguments,
    InvalidEventListener,
    ListenersAlreadyStarted,
    DuplicateListenerName,
)
from lightbus.log import L, Bold
from lightbus.client.commands import (
    SendEventCommand,
    AcknowledgeEventCommand,
    ConsumeEventsCommand,
    CloseCommand,
)
from lightbus.utilities.async_tools import run_user_provided_callable, cancel_and_log_exceptions
from lightbus.utilities.internal_queue import InternalQueue
from lightbus.utilities.casting import cast_to_signature
from lightbus.utilities.deforming import deform_to_bus
from lightbus.utilities.singledispatch import singledispatchmethod

logger = logging.getLogger(__name__)


class EventClient(BaseSubClient):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._event_listeners: List[Listener] = []
        self._event_listener_tasks = set()
        self._listeners_started = False

    async def fire_event(
        self, api_name, name, kwargs: dict = None, options: dict = None
    ) -> EventMessage:
        kwargs = kwargs or {}
        try:
            api = self.api_registry.get(api_name)
        except UnknownApi:
            raise UnknownApi(
                "Lightbus tried to fire the event {api_name}.{name}, but no API named {api_name}"
                " was found in the registry. An API being in the registry implies you are an"
                " authority on that API. Therefore, Lightbus requires the API to be in the registry"
                " as it is a bad idea to fire events on behalf of remote APIs. However, this could"
                " also be caused by a typo in the API name or event name, or be because the API"
                " class has not been registered using bus.client.register_api(). ".format(
                    **locals()
                )
            )

        validate_event_or_rpc_name(api_name, "event", name)

        try:
            event = api.get_event(name)
        except EventNotFound:
            raise EventNotFound(
                "Lightbus tried to fire the event {api_name}.{name}, but the API {api_name} does"
                " not seem to contain an event named {name}. You may need to define the event, you"
                " may also be using the incorrect API. Also check for typos.".format(**locals())
            )

        p: Parameter
        parameter_names = {p.name if isinstance(p, Parameter) else p for p in event.parameters}
        required_parameter_names = {
            p.name if isinstance(p, Parameter) else p
            for p in event.parameters
            if getattr(p, "is_required", True)
        }
        if required_parameter_names and not required_parameter_names.issubset(set(kwargs.keys())):
            raise InvalidEventArguments(
                "Missing required arguments when firing event {}.{}. Attempted to fire event with "
                "{} arguments: {}. Event requires {}: {}".format(
                    api_name,
                    name,
                    len(kwargs),
                    sorted(kwargs.keys()),
                    len(parameter_names),
                    sorted(parameter_names),
                )
            )

        extra_arguments = set(kwargs.keys()) - parameter_names
        if extra_arguments:
            raise InvalidEventArguments(
                "Unexpected argument supplied when firing event {}.{}. Attempted to fire event with"
                " {} arguments: {}. Unexpected argument(s): {}".format(
                    api_name,
                    name,
                    len(kwargs),
                    sorted(kwargs.keys()),
                    sorted(extra_arguments),
                )
            )

        kwargs = deform_to_bus(kwargs)
        event_message = EventMessage(
            api_name=api.meta.name, event_name=name, kwargs=kwargs, version=api.meta.version
        )

        validate_outgoing(self.config, self.schema, event_message)

        await self.hook_registry.execute("before_event_sent", event_message=event_message)
        logger.info(L("📤  Sending event {}.{}".format(Bold(api_name), Bold(name))))

        await self.producer.send(SendEventCommand(message=event_message, options=options)).wait()

        await self.hook_registry.execute("after_event_sent", event_message=event_message)

        return event_message

    def listen(
        self,
        events: List[Tuple[str, str]],
        listener: Callable,
        listener_name: str,
        options: dict = None,
        on_error: OnError = OnError.SHUTDOWN,
    ):
        if self._listeners_started:
            # We are actually technically able to support starting listeners after worker
            # startup, but it seems like it is a bad idea and a bit of an edge case.
            # We may revisit this if sufficient demand arises.
            raise ListenersAlreadyStarted(
                "You are trying to register a new listener after the worker has started running."
                " Listeners should be setup in your @bus.client.on_start() hook, in your bus.py"
                " file."
            )

        sanity_check_listener(listener)

        for listener_api_name, _ in events:
            duplicate_listener = self.get_event_listener(listener_api_name, listener_name)
            if duplicate_listener:
                raise DuplicateListenerName(
                    f"A listener with name '{listener_name}' is already registered for API"
                    f" '{listener_api_name}'. You cannot have multiple listeners with the same name"
                    " for a given API. Rename one of your listeners to resolve this problem."
                )

        for api_name, name in events:
            validate_event_or_rpc_name(api_name, "event", name)

        self._event_listeners.append(
            Listener(
                callable=listener,
                options=options or {},
                events=events,
                name=listener_name,
                on_error=on_error,
            )
        )

    def get_event_listener(self, api_name: str, listener_name: str):
        for listener in self._event_listeners:
            if listener.name == listener_name:
                for listener_api_name, _ in listener.events:
                    if listener_api_name == api_name:
                        return listener
        return None

    async def _on_message(
        self, event_message: EventMessage, listener: Callable, options: dict, on_error: OnError
    ):
        # TODO: Check events match those requested
        logger.info(
            L(
                "📩  Received event {}.{} with ID {}".format(
                    Bold(event_message.api_name), Bold(event_message.event_name), event_message.id
                )
            )
        )

        validate_incoming(self.config, self.schema, event_message)

        await self.hook_registry.execute("before_event_execution", event_message=event_message)

        if self.config.api(event_message.api_name).cast_values:
            parameters = cast_to_signature(parameters=event_message.kwargs, callable=listener)
        else:
            parameters = event_message.kwargs

        # Call the listener.
        # Pass the event message as a positional argument,
        # thereby allowing listeners to have flexibility in the argument names.
        # (And therefore allowing listeners to use the `event` parameter themselves)
        if on_error == OnError.SHUTDOWN:
            # Run the callback in the queue_exception_checker(). This will
            # put any errors into Lightbus' error queue, and therefore
            # cause a shutdown
            await queue_exception_checker(
                run_user_provided_callable(
                    listener,
                    args=[event_message],
                    kwargs=parameters,
                    type_name="listener",
                ),
                self.error_queue,
                help=(
                    f"An error occurred while {listener} was handling an event. Lightbus will now"
                    " shutdown. If you wish to continue you can use the on_error parameter when"
                    " setting up your event. For example:\n\n    bus.my_api.my_event.listen(fn,"
                    " listener_name='example', on_error=lightbus.OnError.ACKNOWLEDGE_AND_LOG)"
                ),
            )
        elif on_error == on_error.ACKNOWLEDGE_AND_LOG:
            try:
                await listener(event_message, **parameters)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # Log here. Acknowledgement will follow in below
                logger.exception(e)

        # Acknowledge the successfully processed message
        await self.producer.send(
            AcknowledgeEventCommand(message=event_message, options=options)
        ).wait()

        await self.hook_registry.execute("after_event_execution", event_message=event_message)

    async def close(self):
        await super().close()
        await cancel_and_log_exceptions(*self._event_listener_tasks)
        await self.producer.send(CloseCommand()).wait()

        await self.consumer.close()
        await self.producer.close()

    @singledispatchmethod
    async def handle(self, command):
        raise NotImplementedError(f"Did not recognise command {command.__class__.__name__}")

    async def start_registered_listeners(self):
        """Start all listeners which have been previously registered via listen()"""
        self._listeners_started = True
        for listener in self._event_listeners:
            await self._start_listener(listener)

    async def _start_listener(self, listener: "Listener"):
        # Setting the maxsize to 1 ensures the transport cannot load
        # messages faster than we can consume them
        queue: InternalQueue[EventMessage] = InternalQueue(maxsize=1)

        async def consume_events():
            while True:
                logger.debug("Event listener now waiting for event on the internal queue")
                event_message = await queue.get()
                logger.debug(
                    "Event listener has now received an event on the internal queue, processing now"
                )
                await self._on_message(
                    event_message=event_message,
                    listener=listener.callable,
                    options=listener.options,
                    on_error=listener.on_error,
                )
                queue.task_done()

        # Start the consume_events() consumer running
        task = asyncio.ensure_future(queue_exception_checker(consume_events(), self.error_queue))
        self._event_listener_tasks.add(task)

        await self.producer.send(
            ConsumeEventsCommand(
                events=listener.events,
                destination_queue=queue,
                listener_name=listener.name,
                options=listener.options,
            )
        ).wait()


class Listener(NamedTuple):
    callable: Callable
    options: dict
    events: List[Tuple[str, str]]
    name: str
    on_error: OnError


def sanity_check_listener(listener):
    if not callable(listener):
        raise InvalidEventListener(
            f"The specified event listener {listener} is not callable. Perhaps you called the"
            " function rather than passing the function itself?"
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
            "This will be the event message. For example: "
            "my_listener(event, other, ...)"
        )
