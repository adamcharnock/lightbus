import argparse
import logging
import sys

from lightbus.plugins import autoload_plugins, plugin_hook
from lightbus.utilities import configure_logging, block
import lightbus.commands.run
import lightbus.commands.shell

logger = logging.getLogger(__name__)


def lightbus_entry_point():  # pragma: no cover
    configure_logging()
    args = parse_args()
    args.func(args)


def parse_args(args=None):
    parser = argparse.ArgumentParser(description='Lightbus management command.')
    subparsers = parser.add_subparsers(help='Commands', dest='subcommand')
    subparsers.required = True

    lightbus.commands.run.Command().setup(parser, subparsers)
    lightbus.commands.shell.Command().setup(parser, subparsers)

    autoload_plugins()
    block(plugin_hook('before_parse_args', parser=parser, subparsers=subparsers), timeout=5)

    args = parser.parse_args(sys.argv[1:] if args is None else args)
    block(plugin_hook('after_parse_args', args=args), timeout=5)

    return args


