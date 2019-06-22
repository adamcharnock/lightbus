import argparse
import logging
import re

import sys
from fnmatch import fnmatch

from lightbus import EventMessage, Api, BusPath
from lightbus.commands.utilities import BusImportMixin, LogLevelMixin
from lightbus.plugins import PluginRegistry
from lightbus.utilities.async_tools import block
from lightbus_vendored.jsonpath import jsonpath

logger = logging.getLogger(__name__)


class Command(LogLevelMixin, BusImportMixin, object):
    """
    Planning

       * --json foo.bar=123,moo.cow=456
       * --id=x
       * --native-id=x
       * --api-name=foo.*
       * --event-name=my_event*
       * --follow
       * --cache-only

    """

    def setup(self, parser, subparsers):
        parser_shell = subparsers.add_parser(
            "inspect",
            help="Inspect events on the bus",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser_shell.add_argument(
            "--json",
            "-j",
            help=("Search event body json for the givn value. Eg. address.city=London"),
            metavar="JSON_SEARCH",
        )
        parser_shell.add_argument(
            "--id",
            "-i",
            help=("Find a single event with this Lightbus event ID"),
            metavar="LIGHTBUS_EVENT_ID",
        )
        parser_shell.add_argument(
            "--native-id",
            "-n",
            help=("Find a single event with this broker-native ID"),
            metavar="NATIVE_EVENT_ID",
        )
        parser_shell.add_argument(
            "--api",
            "-a",
            help=("Find events for this API name. Supports '*' wildcard."),
            metavar="API_NAME",
        )
        parser_shell.add_argument(
            "--event",
            "-e",
            help=("Find events for this event name. Supports '*' wildcard."),
            metavar="EVENT_NAME",
        )
        parser_shell.add_argument(
            "--version",
            "-v",
            help=("Find events with the specified version number. Can be prefixed by <, >, <=, >="),
            metavar="VERSION_NUMBER",
        )
        parser_shell.add_argument(
            "--cache-only",
            "-c",
            help=("Search the local cache only"),
            metavar="EVENT_NAME",
            default=True,
        )

        self.setup_import_parameter(parser_shell)
        parser_shell.set_defaults(func=self.handle)

    def handle(self, args, config, plugin_registry: PluginRegistry):
        self.setup_logging(args.log_level or "warning", config)
        bus_module, bus = self.import_bus(args)

        for api in bus.client.api_registry.public():
            if not args.api or self.wildcard_match(args.api, api.meta.name):
                logger.debug(f"Inspecting {api.meta.name}")
                block(self.search_in_api(args, api, bus))
            else:
                logger.debug(f"API {api.meta.name} did not match {args.api}. Skipping")

    async def search_in_api(self, args, api: Api, bus: BusPath):
        transport = bus.client.transport_registry.get_event_transport(api.meta.name)
        async for message in transport.history():
            if self.match_message(args, message):
                self.output(args, message)

    def wildcard_match(self, pattern: str, s: str) -> bool:
        return fnmatch(s, pattern)

    def version_match(self, pattern: str, version: int) -> bool:
        match = re.match(r"(<|>|<=|>=|!=|=)?(\d+)", pattern)
        if not match:
            sys.stdout.write("Invalid version")
            exit(1)

        comparator, version_ = match.groups()

        try:
            version_ = int(version_)
        except TypeError:
            sys.stdout.write("Invalid version")
            exit(1)

        return self.compare(comparator, left_value=version, right_value=version_)

    def json_match(self, query: str, data: dict) -> bool:
        match = re.match(r"(.*?)(<|>|<=|>=|!=|=)?(.*)", query)
        if not match:
            sys.stdout.write("Invalid json query")
            exit(1)

        query, comparator, value = match.groups()
        found_values = jsonpath(data, query)

        for found_value in found_values:
            if self.compare(comparator, left_value=found_value, right_value=value):
                return True

        return False

    def match_message(self, args, message: EventMessage) -> bool:
        if args.id and args.id != message.id:
            return False

        if args.native_id and args.native_id != message.native_id:
            return False

        if args.event and not self.wildcard_match(args.event, message.event_name):
            return False

        if args.json and not self.json_match(args.json, message.kwargs):
            return False

        if args.version and not self.version_match(args.json, message.version):
            return False

        return True

    def output(self, args, message: EventMessage):
        raise NotImplemented()

    def compare(self, comparator: str, left_value, right_value):
        lookup = {
            "=": lambda: left_value == right_value,
            "!=": lambda: left_value != right_value,
            "<": lambda: left_value < right_value,
            ">": lambda: left_value > right_value,
            "<=": lambda: left_value <= right_value,
            ">=": lambda: left_value >= right_value,
        }
        if comparator not in lookup:
            sys.stdout.write(f"Unknown comparator '{comparator}'")
            exit(1)
        return lookup[comparator]()
