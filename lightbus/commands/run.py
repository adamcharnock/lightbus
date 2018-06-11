import argparse
import asyncio
import logging
from inspect import iscoroutine

from lightbus.commands.utilities import BusImportMixin, LogLevelMixin
from lightbus.exceptions import NoBusFoundInBusModule
from lightbus.utilities.async import block

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

    def handle(self, args, config, dry_run=False):
        self.setup_logging(args.log_level, config)

        bus_module = self.import_bus(args)
        try:
            bus = bus_module.bus
        except AttributeError:
            raise NoBusFoundInBusModule(
                f"Bus module at {bus_module.__file__} contains no variable named 'bus'. "
                f"Your bus module should contain the line 'bus = lightbus.create()'."
            )

        # TODO: Move to lightbus.create()?
        if args.schema:
            if args.schema == "-":
                # if '-' read from stdin
                source = None
            else:
                source = args.schema
            bus.schema.load_local(source)

        before_server_start = getattr(bus_module, "before_server_start", None)
        if before_server_start:
            logger.debug("Calling {}.before_server_start() callback".format(bus_module.__name__))
            co = before_server_start(bus)
            if iscoroutine(co):
                block(co, asyncio.get_event_loop(), timeout=10)

        if dry_run:
            return

        if args.events_only:
            bus.run_forever(consume_rpcs=False)
        else:
            bus.run_forever()
