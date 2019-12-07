from lightbus.transports.base import TransportRegistry
from lightbus.utilities.singledispatch import singledispatchmethod

from lightbus.mediator import commands


class TransportHandler:
    """Handle commands coming from the transport invoker

    These are commands which are being sent by the client for
    consumption by the transport
    """

    def __init__(self, transport_registry: TransportRegistry = None):
        self.transport_registry = transport_registry

    def __call__(self, command):
        return self.handle(command)

    @singledispatchmethod
    async def handle(self, command):
        raise NotImplementedError(f"Did not recognise command {command.__name__}")

    @handle.register
    async def handle_send_event(self, command: commands.SendEventCommand):
        event_transport = self.transport_registry.get_event_transport(command.message.api_name)
        await event_transport.send_event(command.message, options=command.options)
