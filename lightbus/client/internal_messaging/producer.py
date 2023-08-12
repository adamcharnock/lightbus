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
from typing import Optional

from lightbus.client.utilities import queue_exception_checker, ErrorQueueType
from lightbus.utilities.async_tools import cancel
from lightbus.utilities.internal_queue import InternalQueue

logger = logging.getLogger(__name__)


# Was invoker
class InternalProducer:
    """Base invoker class. Puts commands onto a queue

    Note that commands are execute in parallel. If you wish to know when a command has
    been executed you should await the command.on_done event.
    """

    # Warnings will be displayed if a queue grows to be equal to or greater than this size
    size_warning = 5

    # How often should the queue sizes be monitored
    monitor_interval = 0.1

    def __init__(self, name: str, queue: InternalQueue, error_queue: ErrorQueueType):
        """Initialise the invoker

        The callable specified by `on_exception` will be called with a single positional argument,
        which is the exception which occurred. This should take care of shutting down the invoker,
        as well as any other cleanup which needs to happen.
        """
        self.name = name
        self._queue_monitor_task: Optional[asyncio.Task] = None
        self._monitor_ready = asyncio.Event()
        self.queue = queue
        self.error_queue = error_queue

    def start(self):
        """Starts the queue monitor"""
        # fmt: off
        self._queue_monitor_task = asyncio.ensure_future(queue_exception_checker(
            self._queue_monitor(),
            self.error_queue,
        ))
        # fmt: on

    async def close(self):
        if self._queue_monitor_task:
            await cancel(self._queue_monitor_task)
            self._queue_monitor_task = None
            self._monitor_ready = asyncio.Event()

    async def wait_until_ready(self):
        """Wait until this producer is ready to start receiving & handling commands"""
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
                        "Queue in %s has shrunk back down to %s commands.%s",
                        self.name,
                        current_size,
                        everything_ok,
                    )
                elif show_size_warning:
                    logger.warning("Queue in %s now has %s commands.", self.name, current_size)

            previous_size = current_size
            await asyncio.sleep(self.monitor_interval)

    def send(self, command) -> asyncio.Event:
        logger.debug(f"Sending command {command} in producer {self.name}")
        event = asyncio.Event()
        self.queue.put_nowait((command, event))
        return event
