import argparse
import logging
from inspect import isclass

import lightbus
from lightbus.commands.utilities import BusImportMixin

logger = logging.getLogger(__name__)


class Command(BusImportMixin, object):

    def setup(self, parser, subparsers):
        parser_shell = subparsers.add_parser('shell', help='Provide an interactive Lightbus shell', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        self.setup_import_parameter(parser_shell)
        parser_shell.set_defaults(func=self.handle)

    def handle(self, args, dry_run=False):
        try:
            import bpython
            from bpython.curtsies import main as bpython_main
        except ImportError:
            print('Lightbus shell requires bpython. Run `pip install bpython` to install bpython.')
            exit(1)
            return  # noqa

        logger = logging.getLogger('lightbus')
        logger.setLevel(logging.WARNING)

        # TODO: Configuration
        bus_module = self.import_bus(args)
        bus = lightbus.create(plugins={})

        objects = {
            k: v
            for k, v
            in lightbus.__dict__.items()
            if isclass(v)
        }
        objects.update(bus=bus)

        bpython_main(
            args=['-i', '-q'],
            locals_=objects,
            welcome_message='Welcome to the Lightbus shell. Use `bus` to access your bus.',
        )
