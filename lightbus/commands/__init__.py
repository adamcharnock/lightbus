import argparse
import logging
import sys
from asyncio.events import get_event_loop

from lightbus.plugins import autoload_plugins, plugin_hook
from lightbus.utilities import configure_logging, block
import lightbus.commands.run
import lightbus.commands.shell
import lightbus.commands.dump_schema

logger = logging.getLogger(__name__)


def lightbus_entry_point():  # pragma: no cover
    configure_logging()
    args = parse_args()
    args.func(args)


def parse_args(args=None):
    parser = argparse.ArgumentParser(description='Lightbus management command.')
    parser.add_argument('--config', help='Config file to load, JSON or YAML', metavar='FILE')

    subparsers = parser.add_subparsers(help='Commands', dest='subcommand')
    subparsers.required = True

    lightbus.commands.run.Command().setup(parser, subparsers)
    lightbus.commands.shell.Command().setup(parser, subparsers)
    lightbus.commands.dump_schema.Command().setup(parser, subparsers)

    autoload_plugins()

    loop = get_event_loop()
    block(plugin_hook('before_parse_args', parser=parser, subparsers=subparsers), loop, timeout=5)

    args = parser.parse_args(sys.argv[1:] if args is None else args)
    block(plugin_hook('after_parse_args', args=args), loop, timeout=5)

    return args


