import asyncio
import traceback

from lightbus.exceptions import CannotBlockHere
from lightbus.log import LightbusFormatter


async def handle_aio_exceptions(fn, *args, **kwargs):
    try:
        await fn(*args, **kwargs)
    except asyncio.CancelledError:
        pass
    except Exception:
        traceback.print_exc()


def setup_dev_logging():
    import logging

    logger = logging.getLogger('lightbus')
    handler = logging.StreamHandler()

    formatter = LightbusFormatter(fmt={
        'DEBUG': '%(log_color)s%(name)s | %(msg)s',
        'INFO': '%(log_color)s%(name)s | %(msg)s',
        'WARNING': '%(log_color)s%(name)s | WARNING: %(msg)s (%(module)s:%(lineno)d)',
        'ERROR': '%(log_color)s%(name)s | ERROR: %(msg)s (%(module)s:%(lineno)d)',
        'CRITICAL': '%(log_color)s%(name)s | CRITICAL: %(msg)s (%(module)s:%(lineno)d)',
    })
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)


def human_time(seconds: float):
    if seconds > 1:
        return '{} seconds'.format(round(seconds, 2))
    else:
        return '{} milliseconds'.format(round(seconds * 1000, 2))


def block(coroutine, *, timeout):
    loop = asyncio.get_event_loop()
    if loop.is_running():
        raise CannotBlockHere(
            "It appears you have tried to use a plain-old blocking method "
            "from within an event loop. It is not actually possible to do this, "
            "so try using the async version of the method instead (suitably "
            "prefixed with the 'async' keyword)."
        )
    val = loop.run_until_complete(asyncio.wait_for(coroutine, timeout=timeout))
    return val
