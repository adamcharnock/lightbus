from typing import TYPE_CHECKING

from lightbus.utilities.singledispatch import singledispatchmethod

from lightbus.mediator import commands

if TYPE_CHECKING:
    from lightbus.client import BusClient


class ClientHandler:
    """Handle commands coming from the client invoker

    These are commands which are being sent by the transports for
    consumption by the client
    """

    def __init__(self, bus_client: "BusClient"):
        self.bus_client = bus_client

    def __call__(self, command):
        return self.handle(command)

    @singledispatchmethod
    async def handle(self, command):
        raise NotImplementedError(f"Did not recognise command {command.__name__}")

    @handle.register
    async def handle_receive_event(self, command: commands.ReceiveEventCommand):
        try:
            self.bus_client._event_listeners
        except Exception as e:
            await self.handle(commands.ShutdownCommand(exception=e))
