import logging
import os
from contextlib import contextmanager

from typing import List, Dict

_registry: Dict[str, List] = {}
logger = logging.getLogger(__name__)


def test_hook(hook_name: str):
    for callback in _registry.get(hook_name, []):
        logger.warning(f"Executing test hook {hook_name} on hook {callback}")
        callback()


@contextmanager
def register_callback(hook_name, callback):
    _registry.setdefault(hook_name, [])
    _registry[hook_name].append(callback)
    yield
    _registry[hook_name].remove(callback)


@contextmanager
def crash_at(hook_name):
    return register_callback(hook_name, os._exit)
