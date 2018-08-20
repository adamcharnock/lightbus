import argparse
import asyncio
import logging
from inspect import isclass

import lightbus
from lightbus.commands.utilities import BusImportMixin, LogLevelMixin
from lightbus.plugins import plugin_hook
from lightbus.utilities.async import block

logger = logging.getLogger(__name__)


class Command(LogLevelMixin, BusImportMixin, object):

    def setup(self, parser, subparsers):
        parser_shell = subparsers.add_parser(
            "shell",
            help="Provide an interactive Lightbus shell",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        self.setup_import_parameter(parser_shell)
        parser_shell.set_defaults(func=self.handle)

    def handle(self, args, config, fake_it=False):
        self.setup_logging(args.log_level or "warning", config)

        try:
            import bpython
            from bpython.curtsies import main as bpython_main
        except ImportError:  # pragma: no cover
            print("Lightbus shell requires bpython. Run `pip install bpython` to install bpython.")
            exit(1)
            return  # noqa

        logger = logging.getLogger("lightbus")
        logger.setLevel(logging.WARNING)

        bus_module, bus = self.import_bus(args)

        objects = {k: v for k, v in lightbus.__dict__.items() if isclass(v)}
        objects.update(bus=bus)

        block(plugin_hook("receive_args", args=args), timeout=5)

        # Ability to not start up the repl is useful for testing
        if not fake_it:
            bpython_main(
                args=["-i", "-q"],
                locals_=objects,
                welcome_message="Welcome to the Lightbus shell. Use `bus` to access your bus.",
            )
