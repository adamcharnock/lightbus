import asyncio

import logging
import random
from threading import Thread
from typing import Callable

import os

import importlib.util
import traceback
from glob import glob

from pathlib import Path

import sys

from lightbus.exceptions import CannotBlockHere, SuddenDeathException, LightbusShutdownInProgress
from lightbus.log import LightbusFormatter, L, Bold

logger = logging.getLogger(__name__)

_adjectives = [
    'aged', 'ancient', 'autumn', 'billowing', 'bitter', 'black', 'blue', 'bold',
    'broad', 'broken', 'calm', 'cold', 'cool', 'crimson', 'curly', 'damp',
    'dark', 'dawn', 'delicate', 'divine', 'dry', 'empty', 'falling', 'fancy',
    'flat', 'floral', 'fragrant', 'frosty', 'gentle', 'green', 'hidden', 'holy',
    'icy', 'jolly', 'late', 'lingering', 'little', 'lively', 'long', 'lucky',
    'misty', 'morning', 'muddy', 'mute', 'nameless', 'noisy', 'odd', 'old',
    'orange', 'patient', 'plain', 'polished', 'proud', 'purple', 'quiet', 'rapid',
    'raspy', 'red', 'restless', 'rough', 'round', 'royal', 'shiny', 'shrill',
    'shy', 'silent', 'small', 'snowy', 'soft', 'solitary', 'sparkling', 'spring',
    'square', 'steep', 'still', 'summer', 'super', 'sweet', 'throbbing', 'tight',
    'tiny', 'twilight', 'wandering', 'weathered', 'white', 'wild', 'winter', 'wispy',
    'withered', 'yellow', 'young'
]

_nouns = [
    'art', 'band', 'bar', 'base', 'bird', 'block', 'boat', 'bonus',
    'bread', 'breeze', 'brook', 'bush', 'butterfly', 'cake', 'cell', 'cherry',
    'cloud', 'credit', 'darkness', 'dawn', 'dew', 'disk', 'dream', 'dust',
    'feather', 'field', 'fire', 'firefly', 'flower', 'fog', 'forest', 'frog',
    'frost', 'glade', 'glitter', 'grass', 'hall', 'hat', 'haze', 'heart',
    'hill', 'king', 'lab', 'lake', 'leaf', 'limit', 'math', 'meadow',
    'mode', 'moon', 'morning', 'mountain', 'mouse', 'mud', 'night', 'paper',
    'pine', 'poetry', 'pond', 'queen', 'rain', 'recipe', 'resonance', 'rice',
    'river', 'salad', 'scene', 'sea', 'shadow', 'shape', 'silence', 'sky',
    'smoke', 'snow', 'snowflake', 'sound', 'star', 'sun', 'sun', 'sunset',
    'surf', 'term', 'thunder', 'tooth', 'tree', 'truth', 'union', 'unit',
    'violet', 'voice', 'water', 'waterfall', 'wave', 'wildflower', 'wind', 'wood'
]


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
    logger.setLevel(logging.DEBUG)  # config: log_level


def human_time(seconds: float):
    if seconds > 1:
        return '{} seconds'.format(round(seconds, 2))
    else:
        return '{} milliseconds'.format(round(seconds * 1000, 2))


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


def generate_process_name():
    return '{}-{}-{}'.format(random.choice(_adjectives), random.choice(_nouns), random.randint(1, 1000))


def make_file_safe_api_name(api_name):
    """Make an api name safe for use in a file name"""
    return "".join([c for c in api_name if c.isalpha() or c.isdigit() or c in ('.', '_', '-')])


def get_event_loop():
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError as e:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop
