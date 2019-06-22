import argparse
import json
import logging
import re
from hashlib import sha1

import sys
from fnmatch import fnmatch
from pathlib import Path
from typing import Optional

from lightbus import EventMessage, Api, BusPath, EventTransport
from lightbus.commands.utilities import BusImportMixin, LogLevelMixin
from lightbus.plugins import PluginRegistry
from lightbus.utilities.async_tools import block
from lightbus_vendored.jsonpath import jsonpath

CACHE_PATH = Path("~/.lightbus/inspect_cache").expanduser()

logger = logging.getLogger(__name__)


class Command(LogLevelMixin, BusImportMixin, object):
    def setup(self, parser, subparsers):
        parser_shell = subparsers.add_parser(
            "inspect",
            help="Inspect events on the bus",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser_shell.add_argument(
            "--json-path",
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
            type=int,
        )
        parser_shell.add_argument(
            "--format",
            "-F",
            help=("Formatting style. One of pretty, json"),
            metavar="FORMAT",
            default="json",
        )
        parser_shell.add_argument(
            "--cache-only",
            "-c",
            help=("Search the local cache only"),
            metavar="EVENT_NAME",
            default=False,
            type=bool,
        )
        parser_shell.add_argument(
            "--follow",
            "-f",
            help=("Continually listen for new events matching the search criteria"),
            default=True,
            type=bool,
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
        async for message in self.get_messages(args, api, args.event, transport):
            if self.match_message(args, message):
                self.output(args, transport, message)

    async def get_messages(
        self, args, api: Api, event_name: Optional[str], transport: EventTransport
    ):
        CACHE_PATH.mkdir(parents=True, exist_ok=True)
        event_name = event_name or "*"

        # Construct the cache file name
        file_name_hash = sha1((api.meta.name + "\0" + event_name).encode("utf8")).hexdigest()[:8]
        file_name_api = re.sub("[^a-zA-Z0-9_]", "_", api.meta.name)
        file_name_event = event_name.replace("*", "all")

        cache_file_name = f"{file_name_hash}-{file_name_api}-{file_name_event}.json"
        cache_file = CACHE_PATH / cache_file_name

        logger.debug(f"Loading from cache file {cache_file}. Exists: {cache_file.exists()}")

        # Sanity check
        if not cache_file.exists() and args.cache_only:
            sys.stdout.write(
                f"No cache file exists for {api.meta.name}.{event_name}, but --cache-only was specified\n"
            )
            exit(1)

        # Start by reading from cache
        start = None
        if cache_file.exists():
            with cache_file.open() as f:
                for line in f:
                    event_message = transport.deserializer(json.loads(line))

                    if not args.cache_only:
                        if not hasattr(event_message, "datetime"):
                            # Messages do not provide a datetime, stop loading from the cache as
                            # this is required
                            logger.warning(
                                "Event transport does not provided message date times. Will not load from cache."
                            )
                            break
                        start = event_message.datetime

                    yield event_message

        if args.cache_only:
            return

        # Now get messages from the transport, writing to the cache as we go
        with cache_file.open("a") as f:
            async for event_message in transport.history(
                api_name=api.meta.name, event_name=args.event or "*", start=start
            ):
                f.write(json.dumps(transport.serializer(event_message)))
                f.write("\n")
                yield event_message

    def wildcard_match(self, pattern: str, s: str) -> bool:
        return fnmatch(s, pattern)

    def version_match(self, pattern: str, version: int) -> bool:
        """Does the given version match the given pattern"""
        match = re.match(r"(<|>|<=|>=|!=|=)(\d+)", pattern)
        if not match:
            sys.stdout.write("Invalid version\n")
            exit(1)

        comparator, version_ = match.groups()

        try:
            version_ = int(version_)
        except TypeError:
            sys.stdout.write("Invalid version\n")
            exit(1)

        return self.compare(comparator, left_value=version, right_value=version_)

    def json_match(self, query: str, data: dict) -> bool:
        """Does the given data match the given jsonpath query"""
        match = re.match(r"(.+?)(<|>|<=|>=|!=|=)(.+)", query)
        if not match:
            sys.stdout.write("Invalid json query\n")
            exit(1)

        query, comparator, value = match.groups()
        found_values = jsonpath(data, query)

        if not found_values:
            return False

        for found_value in found_values:
            if self.compare(comparator, left_value=found_value, right_value=value):
                return True

        return False

    def match_message(self, args, message: EventMessage) -> bool:
        """Does a message match the given search criteria?"""
        if args.id and args.id != message.id:
            return False

        if args.native_id and args.native_id != message.native_id:
            return False

        if args.event and not self.wildcard_match(args.event, message.event_name):
            return False

        if args.json_path and not self.json_match(args.json_path, message.kwargs):
            return False

        if args.version and not self.version_match(args.json, message.version):
            return False

        return True

    def output(self, args, transport: EventTransport, message: EventMessage):
        """Print out the given message"""

        serialized = transport.serializer(message)
        if args.format == "json":
            print(json.dumps(serialized))

        elif args.format == "pretty":
            print(Colors.BGreen, end="")
            print(f" {message.api_name}.{message.event_name} ".center(80, "="))
            if hasattr(message, "datetime"):
                print(f" {message.datetime.strftime('%c')} ".center(80, " "))
            print(Colors.Reset, end="")

            print(f"\n{Colors.BWhite}Metadata:{Colors.Reset}")
            for k, v in message.get_metadata().items():
                print(f"    {str(k).ljust(20)}: {v}")

            print(f"\n{Colors.BWhite}Data:{Colors.Reset}")
            for k, v in message.get_kwargs().items():
                if isinstance(v, (dict, list)):
                    v = json.dumps(v)
                print(f"    {str(k).ljust(20)}: {v}")

            print("\n")

        else:
            sys.stdout.write(f"Unknown output format '{args.format}'\n")
            exit(1)

    def compare(self, comparator: str, left_value, right_value):
        """Utility for performing arbitrary comparisons"""
        lookup = {
            "=": lambda: left_value == right_value,
            "!=": lambda: left_value != right_value,
            "<": lambda: left_value < right_value,
            ">": lambda: left_value > right_value,
            "<=": lambda: left_value <= right_value,
            ">=": lambda: left_value >= right_value,
        }

        if comparator not in lookup:
            sys.stdout.write(f"Unknown comparator '{comparator}'\n")
            exit(1)

        return lookup[comparator]()


class Colors(object):
    # Reset
    Reset = "\033[0m"  # Text Reset

    # Regular Colors
    Black = "\033[0;30m"
    Red = "\033[0;31m"
    Green = "\033[0;32m"
    Yellow = "\033[0;33m"
    Blue = "\033[0;34m"
    Purple = "\033[0;35m"
    Cyan = "\033[0;36m"
    White = "\033[0;37m"

    # Bold
    BBlack = "\033[1;30m"
    BRed = "\033[1;31m"
    BGreen = "\033[1;32m"
    BYellow = "\033[1;33m"
    BBlue = "\033[1;34m"
    BPurple = "\033[1;35m"
    BCyan = "\033[1;36m"
    BWhite = "\033[1;37m"
