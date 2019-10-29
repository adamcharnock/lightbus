import argparse
import json
import logging

from lightbus.commands.utilities import BusImportMixin, LogLevelMixin
from lightbus.config.config import config_as_json_schema
from lightbus.plugins import PluginRegistry
from lightbus.schema.encoder import json_encode

logger = logging.getLogger(__name__)


class Command(LogLevelMixin, BusImportMixin, object):
    def setup(self, parser, subparsers):
        parser_shell = subparsers.add_parser(
            "dumpconfigschema",
            help=(
                "Dumps the lightbus configuration json schema. Can be useful "
                "in validating your config. This is not the same as your "
                "bus' API schema, for that see the more commonly used 'dumpschema' "
                "command"
            ),
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser_shell.add_argument(
            "--schema",
            "-m",
            help=(
                "File to write config schema to. "
                "If omitted schema will be written to standard out."
            ),
            metavar="FILE",
        )
        self.setup_import_parameter(parser_shell)
        parser_shell.set_defaults(func=self.handle)

    def handle(self, args, config, plugin_registry: PluginRegistry):
        self.setup_logging(args.log_level or "warning", config)

        bus_module, bus = self.import_bus(args)

        schema = json_encode(config_as_json_schema(), indent=2, sort_keys=True)

        if args.schema:
            with open(args.schema, "w", encoding="utf8") as f:
                f.write(schema)
        else:
            print(schema)
