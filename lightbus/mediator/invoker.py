"""


What are we actually trying to glue together here? We're trying to glue
two things together:

* The developer-facing API
* The transport layer

All interactions from user-provided callables will come via the developer-facing API.
We can therefore treat the handling of user-provided callables as a separate system.

What does this flow look like then? For example, if firing an event:

* Developer fires an event
* Client-API places it into an internal queue (optionally along with a asyncio.Condition)
* A consumer picks it up
* The consumer routes it to the correct transport

Notes:

* If we ensure the worker thread actually doesn't block then we will only need one worker thread.
  Currently it blocks, this is bad. We should aim to move the blocking into the
  top level API and not the worker thread.

"""

import asyncio
import logging
import sys
from inspect import iscoroutinefunction
from typing import Optional, NamedTuple, Callable

from lightbus.exceptions import LightbusShutdownInProgress
from lightbus.utilities.async_tools import cancel
from lightbus.mediator import commands


logger = logging.getLogger(__name__)


class Invoker:
    """ Base invoker class. Puts commands onto a queue and executes a handler for each.

    Note that commands are execute in parallel. If you wish to know when a command has
    been executed you should await the command.on_done event.
    """

    # Warnings will be displayed if a queue grows to be equal to or greater than this size
    size_warning = 5

    # How often should the queue sizes be monitored
    monitor_interval = 0.1

    def __init__(self, on_exception: Callable):
        """Initialise the invoker

        The callable specified by `on_exception` will be called with a single positional argument,
        which is the exception which occurred. This should take care of shutting down the invoker,
        as well as any other cleanup which needs to happen.
        """
        self.queue = asyncio.Queue(maxsize=10)
        self.exception_queue = asyncio.Queue()
        self._task: Optional[asyncio.Task] = None
        self._queue_monitor_task: Optional[asyncio.Task] = None
        self._ready = asyncio.Event()
        self._monitor_ready = asyncio.Event()
        self._on_exception = on_exception
        self._running_tasks = set()

    def set_handler_and_start(self, fn):
        """Set the handler function and start the invoker

        Use `stop()` to shutdown the invoker.
        """
        if not iscoroutinefunction(fn):
            raise RuntimeError(f"Handler must be an async function, got: {repr(fn)}")

        self._queue_monitor_task = asyncio.ensure_future(self._queue_monitor())
        self._queue_monitor_task.add_done_callback(self.handle_any_exception)
        self._task = asyncio.ensure_future(self._handle_loop(fn))
        self._task.add_done_callback(self.handle_any_exception)

    async def stop(self, wait_seconds=1):
        """Shutdown the invoker and cancel any currently running tasks

        The shutdown procedure will stop any new tasks for being created,
        then wait `wait_seconds` to allow any running tasks to finish normally.
        Tasks still running after this period will be cancelled.
        """

        # Stop consuming commands from the queue
        # (this will also stop *new* tasks being created)
        if self._task is not None:
            await cancel(self._task)
            self._task = None
            self._ready = asyncio.Event()

        for _ in range(100):
            if self._running_tasks:
                await asyncio.sleep(wait_seconds / 100)

        # Now we have stopped consuming commands we can
        # cancel any running tasks safe in the knowledge that
        # no new tasks will get created
        await cancel(*self._running_tasks)

        # Finally turn off our monitoring
        if self._queue_monitor_task:
            await cancel(self._queue_monitor_task)
            self._queue_monitor_task = None
            self._monitor_ready = asyncio.Event()

    async def wait_until_ready(self):
        """Wait until this invoker is ready to start receiving & handling commands"""
        await self._ready.wait()
        await self._monitor_ready.wait()

    async def _handle_loop(self, fn):
        """Continually fetch commands from the queue and handle them"""
        self._ready.set()

        while True:
            command = await self.queue.get()
            self._handle_command(fn, command)

    def _handle_command(self, fn, command):
        """Handle a received command by calling fn

        This execution happens in the background.
        """

        def when_task_finished(fut: asyncio.Future):
            self._running_tasks.remove(fut)
            self.queue.task_done()
            if hasattr(command, "on_done") and command.on_done:
                command.on_done.set()

        background_call_task = asyncio.ensure_future(fn(command))
        self._running_tasks.add(background_call_task)
        background_call_task.add_done_callback(self.handle_any_exception)
        background_call_task.add_done_callback(when_task_finished)

    async def _queue_monitor(self):
        """Watches queues for growth and reports errors"""
        self._monitor_ready.set()

        previous_size = None
        while True:
            current_size = self.queue.qsize()

            show_size_warning = current_size >= self.size_warning and current_size != previous_size
            queue_has_shrunk = (
                previous_size is not None
                and current_size < previous_size
                and previous_size >= self.size_warning
            )

            if show_size_warning or queue_has_shrunk:
                if self.handler_is_running:
                    handler_message = "Handler is running"
                else:
                    handler_message = "Handler is NOT running"

                if queue_has_shrunk:
                    if not show_size_warning:
                        everything_ok = " Queue is now at an OK size again."
                    else:
                        everything_ok = ""

                    logger.warning(
                        "Queue in %s has shrunk back down to %s commands. %s.%s",
                        self.__class__.__name__,
                        current_size,
                        handler_message,
                        everything_ok,
                    )
                elif show_size_warning:
                    logger.warning(
                        "Queue in %s now has %s commands. %s.",
                        self.__class__.__name__,
                        current_size,
                        handler_message,
                    )

            previous_size = current_size
            await asyncio.sleep(self.monitor_interval)

    @property
    def handler_is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    def handle_any_exception(self, fut: asyncio.Future):
        try:
            exception = fut.exception()
        except asyncio.CancelledError:
            exception = None

        if not exception:
            return

        logger.exception(exception)
        logger.error(
            "An exception occurred while invoking a command. The invoker cannot "
            "sensible handle this so it is exiting the process uncleanly. This "
            "exceptions should be caught in the handlers and dealt with."
        )
        sys.exit(100)


class TransportInvoker(Invoker):
    """ Takes commands from the client and passes them to the transports to be invoked
    """

    def send_to_transports(self, command: NamedTuple):
        assert type(command) in commands.TRANSPORT_COMMANDS
        self.queue.put_nowait(command)
        logger.debug(
            "Sending {} to transports. Queue size is now {}", command.__name__, self.queue.qsize()
        )


class ClientInvoker(Invoker):
    """ Takes commands from the transports and passes them to the client to be invoked
    """

    def send_to_client(self, command: NamedTuple):
        assert type(command) in commands.CLIENT_COMMANDS
        self.queue.put_nowait(command)
        logger.debug(
            "Sending {} to transports. Queue size is now {}", command.__name__, self.queue.qsize()
        )
