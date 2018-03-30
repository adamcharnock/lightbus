import sys
from pathlib import Path
import logging

import importlib.util
import os

from lightbus.log import L, Bold

logger = logging.getLogger(__name__)


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


def autodiscover(bus_path: str=None, search_directory: str='.'):
    if not bus_path:
        logger.debug("Attempting to autodiscover bus.py file")
        bus_path = discover_bus_py(search_directory)
        if not bus_path:
            return None
        logger.debug(L("Found bus.py file at: {}", Bold(bus_path)))
    else:
        bus_path = Path(bus_path)
        logger.debug(L("Using bus.py file at: {}", Bold(bus_path)))

    bus_module_name = prepare_exec_for_file(bus_path)
    logger.debug(L("Going to import {}", Bold(bus_module_name)))
    spec = importlib.util.spec_from_file_location(bus_module_name, str(bus_path))
    bus_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bus_module)
    logger.info(L("Auto-importing bus module '{}'", Bold(bus_module_name)))

    return bus_module
