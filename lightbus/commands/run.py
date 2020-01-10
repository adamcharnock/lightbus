import argparse
import asyncio
import logging
import os
import signal
import sys

from lightbus import BusPath
from lightbus.commands import utilities as command_utilities
from lightbus.plugins import PluginRegistry
from lightbus.utilities.async_tools import block
from lightbus.utilities.features import Feature, ALL_FEATURES
from lightbus.utilities.logging import log_welcome_message

logger = logging.getLogger(__name__)

csv_type = lambda value: [v.strip() for v in value.split(",") if value.split(",")]


class Command:
    def setup(self, parser, subparsers):
        # pylint: disable=attribute-defined-outside-init
        self.all_features = [f.value for f in Feature]
        self.features_str = ", ".join(self.all_features)

        parser_run = subparsers.add_parser(
            "run", help="Run Lightbus", formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )

        group = parser_run.add_argument_group(title="Run command arguments")
        group.add_argument(
            "--only",
            "-o",
            help=f"Only provide the specified features. Comma separated list. Possible values: {self.features_str}",
            type=csv_type,
        )
        group.add_argument(
            "--skip",
            "-k",
            help=(
                f"Provide all except the specified features. Comma separated list. "
                f"Possible values: {self.features_str}"
            ),
            type=csv_type,
        )
        group.add_argument(
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

        command_utilities.setup_common_arguments(parser_run)

    def handle(self, args, config, plugin_registry: PluginRegistry):
        try:
            self._handle(args, config, plugin_registry)
        except Exception as e:
            block(plugin_registry.execute_hook("exception", e=e), timeout=5)
            raise

    def _handle(self, args, config, plugin_registry: PluginRegistry):
        command_utilities.setup_logging(override=getattr(args, "log_level", None), config=config)

        bus: BusPath
        bus_module, bus = command_utilities.import_bus(args)

        # Convert only & skip into a list of features to enable
        if args.only or args.skip:
            if args.only:
                features = args.only
            else:
                features = self.all_features

            for skip_feature in args.skip or []:
                if skip_feature in features:
                    features.remove(skip_feature)
        elif os.environ.get("LIGHTBUS_FEATURES"):
            features = csv_type(os.environ.get("LIGHTBUS_FEATURES"))
        else:
            features = ALL_FEATURES

        bus.client.set_features(features)

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

            logger.debug("Caught signal. Stopping Lightbus worker")
            bus.client.request_shutdown()

        for signal_ in restart_signals:
            asyncio.get_event_loop().add_signal_handler(
                signal_, lambda: asyncio.ensure_future(signal_handler())
            )

        log_welcome_message(
            logger=logger,
            transport_registry=bus.client.transport_registry,
            schema=bus.client.schema,
            plugin_registry=bus.client.plugin_registry,
            config=bus.client.config,
        )

        try:
            block(plugin_registry.execute_hook("receive_args", args=args), timeout=5)
            bus.client.run_forever()

        finally:
            # Cleanup signal handlers
            for signal_ in restart_signals:
                asyncio.get_event_loop().remove_signal_handler(signal_)

        if bus.client.exit_code:
            sys.exit(bus.client.exit_code)
