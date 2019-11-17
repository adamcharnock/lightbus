import argparse
import logging
import sys
from inspect import isclass

import lightbus
from lightbus.commands import utilities as command_utilities
from lightbus.plugins import PluginRegistry
from lightbus.utilities.async_tools import block

logger = logging.getLogger(__name__)


class Command:
    def setup(self, parser, subparsers):
        parser_shell = subparsers.add_parser(
            "shell",
            help="Provide an interactive Lightbus shell",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        command_utilities.setup_common_arguments(parser_shell)
        parser_shell.set_defaults(func=self.handle)

    def handle(self, args, config, plugin_registry: PluginRegistry, fake_it=False):
        command_utilities.setup_logging(args.log_level or "warning", config)

        try:
            # pylint: disable=unused-import,cyclic-import,import-outside-toplevel
            import bpython
            from bpython.curtsies import main as bpython_main
        except ImportError:  # pragma: no cover
            print("Lightbus shell requires bpython. Run `pip install bpython` to install bpython.")
            sys.exit(1)
            return  # noqa

        lightbus_logger = logging.getLogger("lightbus")
        lightbus_logger.setLevel(logging.WARNING)

        bus_module, bus = command_utilities.import_bus(args)
        block(bus.client.lazy_load_now())

        objects = {k: v for k, v in lightbus.__dict__.items() if isclass(v)}
        objects.update(bus=bus)

        block(plugin_registry.execute_hook("receive_args", args=args), timeout=5)

        # Ability to not start up the repl is useful for testing
        if not fake_it:
            bpython_main(
                args=["-i", "-q"],
                locals_=objects,
                welcome_message="Welcome to the Lightbus shell. Use `bus` to access your bus.",
            )
