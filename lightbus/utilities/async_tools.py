import sys
import asyncio
import logging
import threading
from concurrent.futures.thread import ThreadPoolExecutor
from time import time
from typing import Coroutine, TYPE_CHECKING
import datetime

import aioredis

from lightbus.exceptions import CannotBlockHere

if TYPE_CHECKING:
    # pylint: disable=unused-import,cyclic-import,cyclic-import
    from schedule import Job

logger = logging.getLogger(__name__)

PYTHON_VERSION = tuple(sys.version_info)


def block(coroutine: Coroutine, loop=None, *, timeout=None):
    """Call asynchronous code synchronously

    Note that this cannot be used inside an event loop.
    """
    loop = loop or get_event_loop()
    if loop.is_running():
        if hasattr(coroutine, "close"):
            coroutine.close()
        raise CannotBlockHere(
            "It appears you have tried to use a blocking API method "
            "from within an event loop. Unfortunately this is unsupported. "
            "Instead, use the async version of the method."
        )
    try:
        if timeout is None:
            val = loop.run_until_complete(coroutine)
        else:
            val = loop.run_until_complete(asyncio.wait_for(coroutine, timeout=timeout))
    except Exception as e:
        # The intention here is to get sensible stack traces from exceptions within blocking calls
        raise e
    return val


def get_event_loop():
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop


async def cancel(*tasks):
    """Useful for cleaning up tasks in tests"""
    # pylint: disable=broad-except
    ex = None
    for task in tasks:
        if task is None:
            continue

        # Cancel all the tasks any pull out any exceptions
        if not task.cancelled():
            task.cancel()
        try:
            await task
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            # aioredis hides errors which occurred within a pipline
            # by wrapping them up in a PipelineError. So see if that is
            # the case here
            if (
                # An aioredis pipeline error
                isinstance(e, aioredis.PipelineError)
                # Where one of the errors that caused it was a CancelledError
                and len(e.args) > 1
                and asyncio.CancelledError in map(type, e.args[1])
            ):
                pass
            else:
                # If there was an exception, and this is the first
                # exception we've seen, then stash it away for later
                if ex is None:
                    ex = e

    # Now raise the first exception we saw, if any
    if ex:
        raise ex


async def cancel_and_log_exceptions(*tasks):
    """Cancel tasks and log any exceptions

    This is useful when shutting down, when tasks need to be cancelled any anything
    that goes wrong should be logged but will not otherwise be dealt with.
    """
    for task in tasks:
        try:
            await cancel(task)
        except Exception as e:
            # pylint: disable=broad-except
            logger.exception(e)
            logger.error(
                "Error encountered when shutting down task %s. Exception logged, will now move on.",
                task,
            )


async def run_user_provided_callable(callable_, args, kwargs):
    """Run user provided code

    If the callable is blocking (i.e. a regular function) it will be
    moved to its own thread and awaited. The purpose of this thread is to:

        1. Allow other tasks to continue as normal
        2. Allow user code to start its own event loop where needed

    If an async function is provided it will be awaited as-is, and no
    child thread will be created.

    The callable will be called with the given args and kwargs
    """
    if asyncio.iscoroutinefunction(callable_):
        try:
            return await callable_(*args, **kwargs)
        except Exception as e:
            exception = e
    else:
        try:
            thread_pool_executor = ThreadPoolExecutor(
                thread_name_prefix="user_provided_callable_tpe"
            )
            future = asyncio.get_event_loop().run_in_executor(
                executor=thread_pool_executor, func=lambda: callable_(*args, **kwargs)
            )
            return await future
        except Exception as e:
            exception = e

    logger.debug(f"Error in user provided callable: {repr(exception)}")
    raise exception


async def call_every(*, callback, timedelta: datetime.timedelta, also_run_immediately: bool):
    """Call callback every timedelta

    If also_run_immediately is set then the callback will be called before any waiting
    happens. If the callback's result is awaitable then it will be awaited

    Callback execution time is accounted for in the scheduling. If the execution takes
    2 seconds, and timedelta is 10 seconds, then call_every() will wait 8 seconds
    before the subsequent execution.
    """
    first_run = True
    while True:
        start_time = time()
        if not first_run or also_run_immediately:
            await run_user_provided_callable(callback, args=[], kwargs={})
        total_execution_time = time() - start_time
        sleep_time = max(0.0, timedelta.total_seconds() - total_execution_time)
        await asyncio.sleep(sleep_time)
        first_run = False


async def call_on_schedule(callback, schedule: "Job", also_run_immediately: bool):
    first_run = True
    while True:
        schedule._schedule_next_run()

        if not first_run or also_run_immediately:
            schedule.last_run = datetime.datetime.now()
            await run_user_provided_callable(callback, args=[], kwargs={})

        td = schedule.next_run - datetime.datetime.now()
        await asyncio.sleep(td.total_seconds())
        first_run = False
