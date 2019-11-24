import inspect
import logging
import secrets
import string

from typing import Type, NamedTuple  # pylint: disable=unused-import,cyclic-import

import itertools

logger = logging.getLogger(__name__)


def make_from_config_structure(class_name, from_config_method, extra_parameters=tuple()) -> Type:
    """
    Create a new named tuple based on the method signature of from_config_method.

    This is useful when dynamically creating the config structure for Transports
    and Plugins.
    """
    # pylint: disable=exec-used

    code = f"class {class_name}Config(NamedTuple):\n    pass\n"
    variables = dict(p={})

    parameters = inspect.signature(from_config_method).parameters.values()
    for parameter in itertools.chain(parameters, extra_parameters):
        if parameter.name == "config":
            # The config parameter is always passed to from_config() in order to
            # give it access to the global configuration (useful for setting
            # sensible defaults)
            continue

        if parameter.kind in (parameter.POSITIONAL_ONLY, parameter.VAR_POSITIONAL):
            logger.warning(
                f"Positional-only arguments are not supported in from_config() on class {class_name}"
            )
        elif parameter.kind in (parameter.VAR_KEYWORD,):
            logger.warning(
                f"**kwargs-style parameters are not supported in from_config() on class {class_name}"
            )
        else:
            name = parameter.name
            variables["p"][name] = parameter
            code += f"    {name}: p['{name}'].annotation = p['{name}'].default\n"

    globals_ = globals().copy()
    globals_.update(variables)
    exec(code, globals_)  # nosec
    return globals_[f"{class_name}Config"]


def random_name(length: int) -> str:
    """Get a random string suitable for a processes/consumer name"""
    return "".join(secrets.choice(string.ascii_lowercase) for _ in range(length))
