import argparse
import logging

import sys
from pathlib import Path

from lightbus.commands.utilities import BusImportMixin, LogLevelMixin
from lightbus.plugins import PluginRegistry
from lightbus.utilities.async_tools import block

logger = logging.getLogger(__name__)


class Command(LogLevelMixin, BusImportMixin, object):
    def setup(self, parser, subparsers):
        parser_shell = subparsers.add_parser(
            "dumpschema",
            help="Dumps all currently present bus schemas to a file or directory",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser_shell.add_argument(
            "--schema",
            "-m",
            help=(
                "File or directory to write schema to. If a directory is "
                "specified one schema file will be created for each API. "
                "If omitted schema will be written to standard out."
            ),
            metavar="FILE_OR_DIRECTORY",
        )
        self.setup_import_parameter(parser_shell)
        parser_shell.set_defaults(func=self.handle)

    def handle(self, args, config, plugin_registry: PluginRegistry):
        self.setup_logging(args.log_level or "warning", config)

        bus_module, bus = self.import_bus(args)
        block(bus.client.lazy_load_now())
        bus.schema.save_local(args.schema)

        if args.schema:
            sys.stderr.write(
                "Schema for {} APIs saved to {}\n".format(
                    len(bus.schema.api_names), Path(args.schema).resolve()
                )
            )
