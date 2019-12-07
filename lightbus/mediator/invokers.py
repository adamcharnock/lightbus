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
from typing import Optional, NamedTuple

from lightbus.client import commands
from lightbus.utilities.async_tools import cancel

logger = logging.getLogger(__name__)


class Invoker:
    """ Base invoker class. Puts commands onto a queue

    Note that commands are execute in parallel. If you wish to know when a command has
    been executed you should await the command.on_done event.
    """

    # Warnings will be displayed if a queue grows to be equal to or greater than this size
    size_warning = 5

    # How often should the queue sizes be monitored
    monitor_interval = 0.1

    def __init__(self):
        """Initialise the invoker

        The callable specified by `on_exception` will be called with a single positional argument,
        which is the exception which occurred. This should take care of shutting down the invoker,
        as well as any other cleanup which needs to happen.
        """
        self.queue: Optional[asyncio.Queue] = None
        self.exception_queue = asyncio.Queue()
        self._task: Optional[asyncio.Task] = None
        self._queue_monitor_task: Optional[asyncio.Task] = None
        self._ready = asyncio.Event()
        self._monitor_ready = asyncio.Event()
        self._running_tasks = set()

    def set_queue_and_start(self, queue: asyncio.Queue):
        """Set the handler function and start the invoker

        Use `stop()` to shutdown the invoker.
        """
        self.queue = queue
        self._queue_monitor_task = asyncio.ensure_future(self._queue_monitor())
        # TODO: Handle monitoring exception
        # self._queue_monitor_task.add_done_callback(self.handle_any_exception)

    async def stop(self):
        if self._queue_monitor_task:
            await cancel(self._queue_monitor_task)
            self._queue_monitor_task = None
            self._monitor_ready = asyncio.Event()

    async def wait_until_ready(self):
        """Wait until this invoker is ready to start receiving & handling commands"""
        await self._monitor_ready.wait()

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
                if queue_has_shrunk:
                    if not show_size_warning:
                        everything_ok = " Queue is now at an OK size again."
                    else:
                        everything_ok = ""

                    logger.warning(
                        "Queue in %s has shrunk back down to %s commands. %s.",
                        self.__class__.__name__,
                        current_size,
                        everything_ok,
                    )
                elif show_size_warning:
                    logger.warning(
                        "Queue in %s now has %s commands.", self.__class__.__name__, current_size
                    )

            previous_size = current_size
            await asyncio.sleep(self.monitor_interval)


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
