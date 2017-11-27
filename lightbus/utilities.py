import asyncio

import logging
from threading import Thread

import os

import importlib.util
import traceback
from glob import glob

from pathlib import Path

import sys

from lightbus.exceptions import CannotBlockHere
from lightbus.log import LightbusFormatter, L, Bold

logger = logging.getLogger(__name__)


async def handle_aio_exceptions(fn, *args, **kwargs):
    try:
        await fn(*args, **kwargs)
    except asyncio.CancelledError:
        pass
    except Exception:
        traceback.print_exc()


def configure_logging():
    import logging

    logger = logging.getLogger('lightbus')
    handler = logging.StreamHandler()

    formatter = LightbusFormatter(fmt={
        'DEBUG': '%(log_color)s%(name)s | %(msg)s',
        'INFO': '%(log_color)s%(name)s | %(msg)s',
        'WARNING': '%(log_color)s%(name)s | %(msg)s',
        'ERROR': '%(log_color)s%(name)s | ERROR: %(msg)s (%(module)s:%(lineno)d)',
        'CRITICAL': '%(log_color)s%(name)s | CRITICAL: %(msg)s',
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
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

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


def import_from_string(name):
    components = name.split('.')
    mod = __import__(components[0])
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod


def discover_bus_py(directory='.'):
    """Try discover a suitable bus.py file to import"""
    lightbus_directory = Path(__file__).parent.resolve()
    for root, dirs, files in os.walk(directory):
        root = Path(root).resolve()
        if 'bus.py' in files and lightbus_directory != root:
            return root / 'bus.py'


def prepare_exec_for_file(file: Path):
    """Given a filename this will try to calculate the python path, add it
    to the search path and return the actual module name that is expected.

    Full credit to Flask: http://flask.pocoo.org
    """
    module = []

    # Chop off file extensions or package markers
    if file.name == '__init__.py':
        file = file.parent
    elif file.name.endswith('.py'):
        file = file.parent / file.name[:-3]
    else:
        raise Exception('The file provided (%s) does exist but is not a '
                             'valid Python file.  This means that it cannot '
                             'be used as application.  Please change the '
                             'extension to .py' % file)
    file = file.resolve()

    dirpath = file
    while 1:
        dirpath, extra = os.path.split(dirpath)
        module.append(extra)
        if not os.path.isfile(os.path.join(dirpath, '__init__.py')):
            break

    sys.path.insert(0, dirpath)
    return '.'.join(module[::-1])


def autodiscover(directory='.'):
    logger.debug("Attempting to autodiscover bus.py file")
    bus_path = discover_bus_py(directory)
    if not bus_path:
        return None
    logger.debug(L("Found bus.py file at: {}", Bold(bus_path)))
    bus_module_name = prepare_exec_for_file(bus_path)
    logger.debug(L("Going to import {}", Bold(bus_module_name)))
    spec = importlib.util.spec_from_file_location(bus_module_name, str(bus_path))
    bus_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bus_module)
    logger.info(L("No initial import was specified. Using autodiscovered module '{}'", Bold(bus_module_name)))

    return bus_module
