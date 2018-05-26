import argparse
import logging
import sys
from asyncio.events import get_event_loop

from lightbus.config import Config
from lightbus.plugins import autoload_plugins, plugin_hook, remove_all_plugins
from lightbus.utilities.logging import configure_logging
from lightbus.utilities.async import block
import lightbus.commands.run
import lightbus.commands.shell
import lightbus.commands.dump_schema
import lightbus.commands.dump_config_schema

logger = logging.getLogger(__name__)


def lightbus_entry_point():  # pragma: no cover
    configure_logging()
    args = parse_args()
    config = load_config(args)
    args.func(args, config)


def parse_args(args=None):
    parser = argparse.ArgumentParser(description="Lightbus management command.")
    parser.add_argument(
        "--service-name",
        "-s",
        help="Name of service in which this process resides. You should "
        "likely set this in production. Will default to a random string.",
    )
    parser.add_argument(
        "--process-name",
        "-p",
        help="A unique name of this process within the service. Will "
        "default to a random string.",
    )
    parser.add_argument("--config", help="Config file to load, JSON or YAML", metavar="FILE")
    parser.add_argument(
        "--log-level",
        help="Set the log level. Overrides any value set in config. "
        "One of debug, info, warning, critical, exception.",
        metavar="LOG_LEVEL",
    )

    subparsers = parser.add_subparsers(help="Commands", dest="subcommand")
    subparsers.required = True

    lightbus.commands.run.Command().setup(parser, subparsers)
    lightbus.commands.shell.Command().setup(parser, subparsers)
    lightbus.commands.dump_schema.Command().setup(parser, subparsers)
    lightbus.commands.dump_schema.Command().setup(parser, subparsers)
    lightbus.commands.dump_config_schema.Command().setup(parser, subparsers)

    autoload_plugins(config=Config.load_dict({}))

    loop = get_event_loop()
    block(plugin_hook("before_parse_args", parser=parser, subparsers=subparsers), loop, timeout=5)

    args = parser.parse_args(sys.argv[1:] if args is None else args)
    block(plugin_hook("after_parse_args", args=args), loop, timeout=5)

    remove_all_plugins()

    return args


def load_config(args) -> Config:
    if args.config:
        config = Config.load_file(file_path=args.config)
    else:
        config = Config.load_dict({})

    if args.service_name:
        config._config.service_name = args.service_name

    if args.process_name:
        config._config.process_name = args.process_name

    return config
