import asyncio
import traceback
import logging

from lightbus.exceptions import LightbusShutdownInProgress, CannotBlockHere

logger = logging.getLogger(__name__)


async def handle_aio_exceptions(fn):
    try:
        await fn
    except asyncio.CancelledError:
        raise
    except LightbusShutdownInProgress as e:
        logger.info('Shutdown in progress: {}'.format(e))
    except Exception as e:
        logger.exception(e)
        traceback.print_exc()


def block(coroutine, loop, *, timeout):
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
        val = loop.run_until_complete(asyncio.wait_for(coroutine, timeout=timeout))
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
        # Cancel all the tasks any pull out any exceptions
        if not task.cancelled():
            task.cancel()
        try:
            await task
            task.result()
        except asyncio.CancelledError:
            pass

        except Exception as e:
            # If there was an exception, and this is the first
            # exception we've seen, then stash it away for later
            if ex is None:
                ex = e

    # Now raise the first exception we saw, if any
    if ex:
        raise ex
