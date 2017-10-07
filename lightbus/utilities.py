import asyncio
import traceback

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

