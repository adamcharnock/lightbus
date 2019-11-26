import argparse
import logging

from lightbus.commands import utilities as command_utilities
from lightbus.config.config import config_as_json_schema
from lightbus.plugins import PluginRegistry
from lightbus.schema.encoder import json_encode

logger = logging.getLogger(__name__)


class Command:
    def setup(self, parser, subparsers):
        parser_dumpconfigschema = subparsers.add_parser(
            "dumpconfigschema",
            help=(
                "Dumps the lightbus configuration json schema. Can be useful "
                "in validating your config. This is not the same as your "
                "bus' API schema, for that see the more commonly used 'dumpschema' "
                "command"
            ),
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        group = parser_dumpconfigschema.add_argument_group(
            title="Dump config schema command arguments"
        )
        group.add_argument(
            "--out",
            "-o",
            help=(
                "File to write config schema to. "
                "If omitted the schema will be written to standard out."
            ),
            metavar="FILE",
        )
        parser_dumpconfigschema.set_defaults(func=self.handle)

    def handle(self, args):
        schema = json_encode(config_as_json_schema(), indent=2, sort_keys=True)

        if args.out:
            with open(args.out, "w", encoding="utf8") as f:
                f.write(schema)
        else:
            print(schema)
