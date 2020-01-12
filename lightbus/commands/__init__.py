import argparse
import logging
import sys

import lightbus
import lightbus.client
import lightbus.creation
from lightbus.config import Config
from lightbus.exceptions import FailedToImportBusModule
from lightbus.plugins import PluginRegistry
from lightbus.utilities.logging import configure_logging
from lightbus.utilities.async_tools import block
import lightbus.commands.run
import lightbus.commands.shell
import lightbus.commands.dump_schema
import lightbus.commands.dump_config_schema
import lightbus.commands.inspect
import lightbus.commands.version

logger = logging.getLogger(__name__)

COMMAND_PARSED_ARGS = {}

RED = "\033[91m" if sys.stdout.isatty() else ""
RESET = "\033[0m" if sys.stdout.isatty() else ""


def lightbus_entry_point():  # pragma: no cover
    sys.path.insert(0, "")
    configure_logging()
    run_command_from_args()


def run_command_from_args(args=None, **extra):
    parsed_args = parse_args(args)

    # Store the args we received away for later. The
    # bus creation process will use them as overrides.
    # This is a bit of hack to allow command line overrides
    # while having no control over bus instantiation (because
    # the application developer does that in bus.py)
    COMMAND_PARSED_ARGS.clear()
    COMMAND_PARSED_ARGS.update(dict(parsed_args._get_kwargs()))

    if hasattr(parsed_args, "config_file"):
        config = load_config(parsed_args)
        plugin_registry = PluginRegistry()
        plugin_registry.autoload_plugins(config)
    else:
        # Command didn't set up any of the common args (via setup_common_arguments()),
        # which are needed to load up the config. As the command didn't set these up,
        # then assume the command doesn't want to be passed the config & plugin_registry
        config = None
        plugin_registry = None

    try:
        if config is None:
            # Don't pass the config & plugin_registry, see above comment
            parsed_args.func(parsed_args, **extra)
        else:
            parsed_args.func(parsed_args, config, plugin_registry, **extra)

    except FailedToImportBusModule as e:
        sys.stderr.write(f"{RED}{e}{RESET}\n")


def parse_args(args=None):
    parser = argparse.ArgumentParser(description="Lightbus management command.")

    subparsers = parser.add_subparsers(help="Commands", dest="subcommand")
    subparsers.required = True

    # Allow each command to set up its own arguments
    lightbus.commands.run.Command().setup(parser, subparsers)
    lightbus.commands.shell.Command().setup(parser, subparsers)
    lightbus.commands.dump_schema.Command().setup(parser, subparsers)
    lightbus.commands.dump_config_schema.Command().setup(parser, subparsers)
    lightbus.commands.inspect.Command().setup(parser, subparsers)
    lightbus.commands.version.Command().setup(parser, subparsers)

    # Create a temporary plugin registry in order to run the before_parse_args hook
    plugin_registry = PluginRegistry()
    plugin_registry.autoload_plugins(config=Config.load_dict({}))

    block(
        plugin_registry.execute_hook("before_parse_args", parser=parser, subparsers=subparsers),
        timeout=5,
    )
    args = parser.parse_args(sys.argv[1:] if args is None else args)
    # Note that we don't have an after_parse_args plugin hook. Instead we use the receive_args
    # hook which is called once we have instantiated our plugins

    return args


def load_config(args) -> Config:
    return lightbus.creation.load_config(
        from_file=args.config_file,
        service_name=args.service_name,
        process_name=args.process_name,
        quiet=True,
    )
