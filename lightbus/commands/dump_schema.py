import argparse
import logging

import sys
from pathlib import Path

from lightbus.commands import utilities as command_utilities
from lightbus.plugins import PluginRegistry
from lightbus.utilities.async_tools import block

logger = logging.getLogger(__name__)


class Command:
    def setup(self, parser, subparsers):
        parser_dumpschema = subparsers.add_parser(
            "dumpschema",
            help="Dumps all currently present bus schemas to a file or directory",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        group = parser_dumpschema.add_argument_group(title="Dump config schema command arguments")
        group.add_argument(
            "--out",
            "-o",
            help=(
                "File or directory to write schema to. If a directory is "
                "specified one schema file will be created for each API. "
                "If omitted the schema will be written to standard out."
            ),
            metavar="FILE_OR_DIRECTORY",
        )
        command_utilities.setup_common_arguments(parser_dumpschema)
        parser_dumpschema.set_defaults(func=self.handle)

    def handle(self, args, config, plugin_registry: PluginRegistry):
        command_utilities.setup_logging(args.log_level or "warning", config)

        bus_module, bus = command_utilities.import_bus(args)
        block(bus.client.lazy_load_now())
        bus.schema.save_local(args.out)

        if args.out:
            sys.stderr.write(
                "Schema for {} APIs saved to {}\n".format(
                    len(bus.schema.api_names), Path(args.out).resolve()
                )
            )
