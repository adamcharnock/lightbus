import importlib
import logging
import sys
from typing import Sequence, Tuple, Callable

if sys.version_info < (3, 10):
    from importlib.metadata import entry_points as _entry_points

    def entry_points(group):
        return _entry_points()[group]

else:
    from importlib.metadata import entry_points


logger = logging.getLogger(__name__)


def import_module_from_string(name):
    """Import a module if necessary, otherwise return it from the list of already imported modules"""
    if name in sys.modules:
        return sys.modules[name]
    else:
        return importlib.import_module(name)


def import_from_string(name):
    components = name.split(".")
    mod = __import__(components[0])
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod


def load_entrypoint_classes(entrypoint_name) -> Sequence[Tuple[str, str, Callable]]:
    """Load classes specified in an entrypoint

    Entrypoints are specified in pyproject.toml, and Lightbus uses them to
    discover plugins & transports.
    """
    found_classes = []
    for entrypoint in entry_points(group=entrypoint_name):
        class_ = entrypoint.load()
        found_classes.append((entrypoint.module, entrypoint.name, class_))
    return found_classes
