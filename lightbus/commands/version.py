import argparse
import logging

import sys
from pathlib import Path

import pkg_resources

from lightbus.commands import utilities as command_utilities
from lightbus.plugins import PluginRegistry
from lightbus.utilities.async_tools import block

logger = logging.getLogger(__name__)


class Command:
    def setup(self, parser, subparsers):
        parser_version = subparsers.add_parser(
            "version", help="Show the currently installed Lightbus version"
        )
        parser_version.set_defaults(func=self.handle)

    def handle(self, args):
        print(pkg_resources.get_distribution("lightbus").version)
