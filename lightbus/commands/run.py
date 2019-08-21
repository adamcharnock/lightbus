import argparse
import asyncio
import logging
import signal
import sys

from lightbus.commands.utilities import BusImportMixin, LogLevelMixin
from lightbus.plugins import PluginRegistry
from lightbus.utilities.async_tools import block
from lightbus.utilities.features import Feature

logger = logging.getLogger(__name__)


class Command(LogLevelMixin, BusImportMixin, object):
    def setup(self, parser, subparsers):
        self.all_features = [f.value for f in Feature]
        self.features_str = ", ".join(self.all_features)
        csv_type = lambda value: [v.strip() for v in value.split(",") if value.split(",")]

        parser_run = subparsers.add_parser(
            "run", help="Run Lightbus", formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        self.setup_import_parameter(parser_run)

        parser_run_action_group = parser_run.add_mutually_exclusive_group()
        parser_run_action_group.add_argument(
            "--only",
            "-o",
            help=f"Only provide the specified features. Comma separated list. Possible values: {self.features_str}",
            type=csv_type,
        )
        parser_run_action_group.add_argument(
            "--skip",
            "-s",
            help=f"Provide all except the specified features. Comma separated list. Possible values: {self.features_str}",
            type=csv_type,
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

        # Convert only & skip into a list of features to enable
        if args.only:
            features = args.only
        else:
            features = self.all_features

        for skip_feature in args.skip or []:
            if skip_feature in features:
                features.remove(skip_feature)

        for i, feature in enumerate(features):
            try:
                features[i] = Feature(feature)
            except ValueError:
                logger.error(f"Feature {feature} is not one of: {self.features_str}\n")
                sys.exit(1)

        # TODO: Move to lightbus.create()?
        if args.schema:
            if args.schema == "-":
                # if '-' read from stdin
                source = None
            else:
                source = args.schema
            bus.schema.load_local(source)

        restart_signals = (signal.SIGINT, signal.SIGTERM)

        # Handle incoming signals
        async def signal_handler():
            # Stop handling signals now. If we receive the signal again
            # let the process quit naturally
            for signal_ in restart_signals:
                asyncio.get_event_loop().remove_signal_handler(signal_)

            logger.debug("Caught signal. Stopping main thread event loop")
            bus.client.shutdown_server(exit_code=0)

        for signal_ in restart_signals:
            asyncio.get_event_loop().add_signal_handler(
                signal_, lambda: asyncio.ensure_future(signal_handler())
            )

        try:
            block(plugin_registry.execute_hook("receive_args", args=args), timeout=5)
            bus.client.run_forever(features=features)

        finally:
            # Cleanup signal handlers
            for signal_ in restart_signals:
                asyncio.get_event_loop().remove_signal_handler(signal_)

        if bus.client.exit_code:
            sys.exit(bus.client.exit_code)
