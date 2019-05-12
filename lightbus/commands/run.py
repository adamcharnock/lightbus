import argparse
import logging

from lightbus.commands.utilities import BusImportMixin, LogLevelMixin
from lightbus.plugins import PluginRegistry
from lightbus.utilities.async_tools import block

logger = logging.getLogger(__name__)


class Command(LogLevelMixin, BusImportMixin, object):
    def setup(self, parser, subparsers):
        parser_run = subparsers.add_parser(
            "run", help="Run Lightbus", formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        self.setup_import_parameter(parser_run)

        parser_run_action_group = parser_run.add_mutually_exclusive_group()
        parser_run_action_group.add_argument(
            "--events-only",
            "-E",
            help="Only listen for and handle events, do not respond to RPC calls",
            action="store_true",
        )
        parser_run_action_group.add_argument(
            "--schema",
            "-m",
            help=(
                "Manually load the schema from the given file or directory. "
                "This will normally be provided by the schema transport, "
                "but manual loading may be useful during development or testing."
            ),
            metavar="FILE_OR_DIRECTORY",
        )
        parser_run.set_defaults(func=self.handle)

    def handle(self, args, config, plugin_registry: PluginRegistry):
        try:
            self._handle(args, config, plugin_registry)
        except Exception as e:
            block(plugin_registry.execute_hook("exception", e=e), timeout=5)
            raise

    def _handle(self, args, config, plugin_registry: PluginRegistry):
        self.setup_logging(override=getattr(args, "log_level", None), config=config)

        bus_module, bus = self.import_bus(args)

        # TODO: Move to lightbus.create()?
        if args.schema:
            if args.schema == "-":
                # if '-' read from stdin
                source = None
            else:
                source = args.schema
            bus.schema.load_local(source)

        block(plugin_registry.execute_hook("receive_args", args=args), timeout=5)

        if args.events_only:
            bus.client.run_forever(consume_rpcs=False)
        else:
            bus.client.run_forever()
