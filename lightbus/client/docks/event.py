import asyncio

from lightbus.client.docks.base import BaseDock
from lightbus.client.utilities import queue_exception_checker
from lightbus.client import commands
from lightbus.utilities.async_tools import cancel
from lightbus.utilities.singledispatch import singledispatchmethod


class EventDock(BaseDock):
    """ Takes internal Lightbus commands and performs interactions with the Event transport
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.listener_tasks = set()

    @singledispatchmethod
    async def handle(self, command):
        raise NotImplementedError(f"Did not recognise command {command.__name__}")

    @handle.register
    async def handle_consume_events(self, command: commands.ConsumeEventsCommand):
        event_transports = self.transport_registry.get_event_transports(
            api_names=[api_name for api_name, _ in command.events]
        )

        async def listener(event_transport, events):
            consumer = await event_transport.consume(
                listen_for=events, listener_name=command.listener_name, **command.options
            )
            async for event_messages in consumer:
                for event_message in event_messages:
                    await command.destination_queue.put(event_message)

        tasks = []
        for _event_transport, _api_names in event_transports:
            # Create a listener task for each event transport,
            # passing each a list of events for which it should listen
            events = [
                (api_name, event_name)
                for api_name, event_name in command.events
                if api_name in _api_names
            ]

            task = asyncio.ensure_future(listener(_event_transport, events))
            task.is_listener = True  # Used by close()
            tasks.append(task)

        listener_task = asyncio.gather(*tasks)

        exception_checker = queue_exception_checker(queue=self.fatal_errors)
        listener_task.add_done_callback(exception_checker)

        # Setting is_listener lets Client.close() know that it should mop up this
        # task automatically on shutdown
        listener_task.is_listener = True

        self.listener_tasks.add(listener_task)

    @handle.register
    async def handle_consume_events(self, command: commands.CloseCommand):
        await cancel(*self.listener_tasks)

        for event_transport in self.transport_registry.get_event_transports():
            await event_transport.close()
