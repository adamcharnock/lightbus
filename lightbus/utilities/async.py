import asyncio
import traceback
import logging
from functools import partial
from inspect import isawaitable
from typing import Coroutine

import aioredis
from aioredis import ConnectionForcedCloseError

from lightbus.exceptions import LightbusShutdownInProgress, CannotBlockHere

logger = logging.getLogger(__name__)


def block(coroutine: Coroutine, loop: asyncio.AbstractEventLoop, *, timeout):
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
        except (asyncio.CancelledError, aioredis.ConnectionForcedCloseError):
            pass

        except Exception as e:
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
    except (asyncio.CancelledError, ConnectionForcedCloseError, LightbusShutdownInProgress):
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
