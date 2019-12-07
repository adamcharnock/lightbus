import asyncio
from typing import Optional

from lightbus.client.commands import ShutdownCommand
from lightbus.mediator.invokers import ClientInvoker
from lightbus.transports.base import TransportRegistry
from lightbus.utilities.async_tools import cancel
from lightbus.utilities.singledispatch import singledispatchmethod

from lightbus.client import commands


class Handler:
    """Handle commands coming from the transport invoker

    These are commands which are being sent by the client for
    consumption by the transport
    """

    def __init__(self):
        self._consumer_task: Optional[asyncio.Task] = None
        self._running_commands = set()
        self._ready = asyncio.Event()

    def set_queue_and_start(self, queue: asyncio.Queue):
        """Set the handler function and start the invoker

        Use `stop()` to shutdown the invoker.
        """
        self._consumer_task = asyncio.ensure_future(self._handle_loop(queue))
        self._consumer_task.add_done_callback(self.handle_any_exception)
        self._running_commands = set()

    async def stop(self, wait_seconds=1):
        """Shutdown the invoker and cancel any currently running tasks

        The shutdown procedure will stop any new tasks for being created,
        then wait `wait_seconds` to allow any running tasks to finish normally.
        Tasks still running after this period will be cancelled.
        """

        # Stop consuming commands from the queue
        # (this will also stop *new* tasks being created)
        if self._consumer_task is not None:
            await cancel(self._consumer_task)
            self._consumer_task = None
            self._ready = asyncio.Event()

        for _ in range(100):
            if self._running_commands:
                await asyncio.sleep(wait_seconds / 100)

        # Now we have stopped consuming commands we can
        # cancel any running tasks safe in the knowledge that
        # no new tasks will get created
        await cancel(*self._running_commands)
        self._running_commands = set()

    async def _handle_loop(self, queue):
        """Continually fetch commands from the queue and handle them"""
        self._ready.set()

        while True:
            on_done: asyncio.Event
            command, on_done = await queue.get()
            self.handle_in_background(queue, command, on_done)

    def handle_in_background(self, queue: asyncio.Queue, command, on_done: asyncio.Event):
        """Handle a received command by calling self.handle

        This execution happens in the background.
        """

        def when_task_finished(fut: asyncio.Future):
            self._running_commands.remove(fut)
            queue.task_done()
            on_done.set()

        background_call_task = asyncio.ensure_future(self.handle(command))
        self._running_commands.add(background_call_task)
        background_call_task.add_done_callback(self.handle_any_exception)
        background_call_task.add_done_callback(when_task_finished)

    async def handle(self, command):
        raise NotImplementedError()

    def handle_any_exception(self, fut: asyncio.Future):
        try:
            exception = fut.exception()
        except Exception as e:
            # Can throw a CancelledException
            exception = e

        if not exception:
            return

        # TODO: Send this exception somewhere? Or just explode?


class TransportHandler(Handler):
    def __init__(self, client_invoker: ClientInvoker, transport_registry: TransportRegistry):
        super().__init__()
        self.client_invoker = client_invoker
        self.transport_registry = transport_registry

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


class ClientHandler(Handler):
    @singledispatchmethod
    async def handle(self, command):
        raise NotImplementedError(f"Did not recognise command {command.__name__}")

    @handle.register
    async def handle_send_event(self, command: commands.ReceiveEventCommand):
        raise NotImplementedError()  # TODO
