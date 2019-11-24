import sys
import asyncio
import logging
import threading
import traceback
from contextlib import contextmanager
from functools import partial
from time import time
from typing import Coroutine, TYPE_CHECKING
import datetime

import aioredis

from lightbus.exceptions import LightbusShutdownInProgress, CannotBlockHere

if TYPE_CHECKING:
    # pylint: disable=unused-import,cyclic-import,cyclic-import
    from schedule import Job
    from lightbus.client import BusClient

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
            val = loop.run_until_complete(asyncio.wait_for(coroutine, timeout=timeout, loop=loop))
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


def check_for_exception(fut: asyncio.Future, bus_client: "BusClient", die=True):
    """Check for exceptions in returned future

    To be used as a callback, eg:

    task.add_done_callback(check_for_exception)
    """
    with exception_handling_context(bus_client, die):
        if fut.exception():
            fut.result()


def make_exception_checker(bus_client: "BusClient", die=True):
    """Creates a callback handler (i.e. check_for_exception())
    which will be called with the given arguments"""
    return partial(check_for_exception, die=die, bus_client=bus_client)


@contextmanager
def exception_handling_context(bus_client: "BusClient", die=True):
    """Handle exceptions in user code"""
    try:
        yield
    except (asyncio.CancelledError, LightbusShutdownInProgress):
        return
    except Exception as e:
        # Must log the exception here, otherwise exceptions that occur during
        # listener setup will never be logged
        logger.debug(f"Exception occurred in exception handling context. die is {die}")
        logger.exception(e)
        if die:
            logger.debug("Stopping event loop and setting exit code")
            bus_client.shutdown_server(exit_code=1)


async def run_user_provided_callable(
    callable_, args, kwargs, bus_client: "BusClient", die_on_exception=True
):
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
        return await callable_(*args, **kwargs)

    # Used to provide helpful output in case of deadlock in client worker
    if hasattr(callable_, "_parent_stack"):
        callable_._parent_stack = traceback.extract_stack(limit=5)[:-1]

    with exception_handling_context(bus_client, die=die_on_exception):
        future = asyncio.get_event_loop().run_in_executor(
            executor=None, func=lambda: callable_(*args, **kwargs)
        )
    return await future


async def call_every(
    *, callback, timedelta: datetime.timedelta, also_run_immediately: bool, bus_client: "BusClient"
):
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
            await run_user_provided_callable(callback, args=[], kwargs={}, bus_client=bus_client)
        total_execution_time = time() - start_time
        sleep_time = max(0.0, timedelta.total_seconds() - total_execution_time)
        await asyncio.sleep(sleep_time)
        first_run = False


async def call_on_schedule(
    callback, schedule: "Job", also_run_immediately: bool, bus_client: "BusClient"
):
    first_run = True
    while True:
        schedule._schedule_next_run()

        if not first_run or also_run_immediately:
            schedule.last_run = datetime.datetime.now()
            await run_user_provided_callable(callback, args=[], kwargs={}, bus_client=bus_client)

        td = schedule.next_run - datetime.datetime.now()
        await asyncio.sleep(td.total_seconds())
        first_run = False


class ThreadSerializedTask(asyncio.Task):
    _lock = threading.Lock()

    def _wakeup(self, *args, **kwargs):
        with ThreadSerializedTask._lock:
            super()._wakeup(*args, **kwargs)

    @staticmethod
    def factory(loop, coro):
        return ThreadSerializedTask(coro, loop=loop)


class LightbusEventLoopPolicy(asyncio.DefaultEventLoopPolicy):
    def get_event_loop(self):
        loop = super().get_event_loop()
        loop.set_task_factory(ThreadSerializedTask.factory)
        return loop


def configure_event_loop():
    asyncio.set_event_loop_policy(LightbusEventLoopPolicy())
