import argparse
import logging

import lightbus
from lightbus.commands.utilities import BusImportMixin

logger = logging.getLogger(__name__)


class Command(BusImportMixin, object):

    def setup(self, parser, subparsers):
        parser_shell = subparsers.add_parser('dumpschema',
                                             help='Dump the bus schema to a file',
                                             formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser_shell.add_argument('--file', help='File or directory to write schema to. If a directory is '
                                                 'specified one schema file will be created for each API. '
                                                 'If omitted schema will be written to standard out.')
        self.setup_import_parameter(parser_shell)
        parser_shell.set_defaults(func=self.handle)

    def handle(self, args):
        self.import_bus(args)
        bus = lightbus.create()
        bus.schema.dump(args.file)
