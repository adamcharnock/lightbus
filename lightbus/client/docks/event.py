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
        raise NotImplementedError(f"Did not recognise command {command.__class__.__name__}")

    @handle.register
    async def handle_consume_events(self, command: commands.ConsumeEventsCommand):
        event_transports = self.transport_registry.get_event_transports(
            api_names=[api_name for api_name, _ in command.events]
        )

        async def listener(event_transport, events_):
            consumer = event_transport.consume(
                listen_for=events_,
                listener_name=command.listener_name,
                error_queue=self.error_queue,
                **command.options,
            )
            async for event_messages in consumer:
                for event_message in event_messages:
                    await command.destination_queue.put(event_message)

                # Wait for the queue to be emptied before fetching more.
                # We will need to make this configurable if we want to support
                # the pre-fetching of events. This solution is a good default
                # though as it will ensure events are processed in a more ordered fashion
                # in cases where there are multiple workers, and also ensure fewer
                # messages need to be reclaimed in the event of a crash
                await command.destination_queue.join()

        for event_transport_pool, api_names in event_transports.items():
            # Create a listener task for each event transport,
            # passing each a list of events for which it should listen
            events = [
                (api_name, event_name)
                for api_name, event_name in command.events
                if api_name in api_names
            ]

            # fmt: off
            listener_task = asyncio.ensure_future(queue_exception_checker(
                listener(event_transport_pool, events),
                self.error_queue,
            ))
            # fmt: on
            self.listener_tasks.add(listener_task)

    @handle.register
    async def handle_send_event(self, command: commands.SendEventCommand):
        event_transport = self.transport_registry.get_event_transport(command.message.api_name)

        await event_transport.send_event(event_message=command.message, options=command.options)

    @handle.register
    async def handle_acknowledge_event(self, command: commands.AcknowledgeEventCommand):
        event_transport = self.transport_registry.get_event_transport(command.message.api_name)
        await event_transport.acknowledge(command.message)

    @handle.register
    async def handle_close(self, command: commands.CloseCommand):
        await cancel(*self.listener_tasks)

        for event_transport in self.transport_registry.get_all_event_transports():
            await event_transport.close()

        await self.consumer.close()
        await self.producer.close()
