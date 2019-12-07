from lightbus.mediator.commands import ShutdownCommand
from lightbus.mediator.invoker import ClientInvoker
from lightbus.transports.base import TransportRegistry
from lightbus.utilities.singledispatch import singledispatchmethod

from lightbus.mediator import commands


class TransportHandler:
    """Handle commands coming from the transport invoker

    These are commands which are being sent by the client for
    consumption by the transport
    """

    def __init__(self, client_invoker: ClientInvoker, transport_registry: TransportRegistry):
        self.client_invoker = client_invoker
        self.transport_registry = transport_registry

    def __call__(self, command):
        return self.handle(command)

    @singledispatchmethod
    async def handle(self, command):
        raise NotImplementedError(f"Did not recognise command {command.__name__}")

    @handle.register
    async def handle_send_event(self, command: commands.SendEventCommand):
        try:
            event_transport = self.transport_registry.get_event_transport(command.message.api_name)
            await event_transport.send_event(command.message, options=command.options)
        except Exception as e:
            self.client_invoker.send_to_client(ShutdownCommand(exception=e))
