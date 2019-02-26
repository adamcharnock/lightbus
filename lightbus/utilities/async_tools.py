import asyncio
import inspect
import logging
from functools import partial
from inspect import isawaitable
from time import time
from typing import Coroutine
from datetime import timedelta, datetime

import aioredis

if False:
    from schedule import Job

from lightbus.exceptions import LightbusShutdownInProgress, CannotBlockHere

logger = logging.getLogger(__name__)


def block(coroutine: Coroutine, loop=None, *, timeout=None):
    loop = loop or get_event_loop()
    if loop.is_running():
        coroutine.close()
        raise CannotBlockHere(
            "It appears you have tried to use a blocking API method "
            "from within an event loop. Unfortunately this is unsupported. "
            "Instead, use the async version of the method. This commonly "
            "occurs when calling bus methods from within a bus event listener. "
            "In this case the only option is to define you listeners as async."
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
    except RuntimeError as e:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop


async def cancel(*tasks):
    """Useful for cleaning up tasks in tests"""
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


def check_for_exception(fut: asyncio.Future, die=True):
    """Check for exceptions in returned future

    To be used as a callback, eg:

    task.add_done_callback(check_for_exception)
    """
    try:
        if fut.exception():
            fut.result()
    except (asyncio.CancelledError, LightbusShutdownInProgress):
        return
    except Exception as e:
        # Must log the exception here, otherwise exceptions that occur during
        # listener setup will never be logged
        logger.exception(e)
        if die:
            loop = fut._loop
            loop.lightbus_exit_code = 1
            loop.stop()


def make_exception_checker(die=True):
    """Creates a callback handler (i.e. check_for_exception())
    which will be called with the given arguments"""
    return partial(check_for_exception, die=die)


async def await_if_necessary(value):
    if isawaitable(value):
        return await value
    else:
        return value


async def execute_in_thread(callable, args, kwargs, bus_client):
    """Execute the given callable in a thread, and await the result

    If the callable returns an awaitable, then it will be awaited.
    The callable may therefore be a coroutine or a regular function.

    The callable will be called with the given args and kwargs
    """

    def make_func(callable, args, kwargs):
        async def coro_wrapper(result):
            await result
            await bus_client._cleanup_thread()

        def wrapper():
            result = callable(*args, **kwargs)
            if inspect.isawaitable(result):
                loop = asyncio.new_event_loop()
                result = loop.run_until_complete(result)
            return result

        return wrapper

    return await asyncio.get_event_loop().run_in_executor(
        executor=None, func=make_func(callable, args, kwargs)
    )


async def call_every(*, callback, timedelta: timedelta, also_run_immediately: bool):
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
            await execute_in_thread(callback, args=[], kwargs={})
        total_execution_time = time() - start_time
        sleep_time = max(0.0, timedelta.total_seconds() - total_execution_time)
        await asyncio.sleep(sleep_time)
        first_run = False


async def call_on_schedule(callback, schedule: "Job", also_run_immediately: bool):
    first_run = True
    while True:
        schedule._schedule_next_run()

        if not first_run or also_run_immediately:
            schedule.last_run = datetime.now()
            await execute_in_thread(callback, args=[], kwargs={})

        td = schedule.next_run - datetime.now()
        await asyncio.sleep(td.total_seconds())
        first_run = False
