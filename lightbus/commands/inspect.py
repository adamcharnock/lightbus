import argparse
import json
import logging
import re
from asyncio import sleep
from hashlib import sha1

import sys
from fnmatch import fnmatch
from pathlib import Path
from typing import Optional, List, get_type_hints

from lightbus import EventMessage, BusPath, EventTransport, ValidationError
from lightbus.commands import utilities as command_utilities
from lightbus.plugins import PluginRegistry
from lightbus.utilities.async_tools import block
from lightbus.utilities.casting import cast_to_signature
from lightbus_vendored.jsonpath import jsonpath


CACHE_PATH = Path("~/.lightbus/inspect_cache").expanduser()

logger = logging.getLogger(__name__)


class Command:
    def setup(self, parser, subparsers):
        parser_inspect = subparsers.add_parser(
            "inspect",
            help="Inspect events on the bus",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        group = parser_inspect.add_argument_group(title="Inspect command arguments")
        group.add_argument(
            "--json-path",
            "-j",
            help="Search event body json for the givn value. Eg. address.city=London",
            metavar="JSON_SEARCH",
        )
        group.add_argument(
            "--id",
            "-i",
            help="Find a single event with this Lightbus event ID",
            metavar="LIGHTBUS_EVENT_ID",
        )
        group.add_argument(
            "--native-id",
            "-n",
            help="Find a single event with this broker-native ID",
            metavar="NATIVE_EVENT_ID",
        )
        group.add_argument(
            "--api",
            "-a",
            help="Find events for this API name. Supports the '*' wildcard.",
            metavar="API_NAME",
        )
        group.add_argument(
            "--event",
            "-e",
            help="Find events for this event name. Supports the '*' wildcard.",
            metavar="EVENT_NAME",
        )
        group.add_argument(
            "--version",
            "-v",
            help="Find events with the specified version number. Can be prefixed by <, >, <=, >=",
            metavar="VERSION_NUMBER",
            type=int,
        )
        group.add_argument(
            "--format",
            "-F",
            help="Formatting style. One of json, pretty, or human.",
            metavar="FORMAT",
            default="json",
            choices=("json", "pretty", "human"),
        )
        group.add_argument(
            "--cache-only", "-c", help="Search the local cache only", action="store_true"
        )
        group.add_argument(
            "--follow",
            "-f",
            help=(
                "Continually listen for new events matching the search criteria. "
                "May only be used on a single API"
            ),
            action="store_true",
        )
        group.add_argument(
            "--validate",
            "-d",
            help=(
                "Validate displayed events against the bus schema. Only available "
                "when --format is set to 'human' (experimental)"
            ),
            action="store_true",
        )
        group.add_argument(
            "--show-casting",
            "-C",
            help=(
                "Show how the message values will be casted for each event listener (experimental)"
            ),
            action="store_true",
        )
        group.add_argument("--internal", "-I", help="Include internal APIs", action="store_true")

        command_utilities.setup_common_arguments(parser_inspect)
        parser_inspect.set_defaults(func=self.handle)

    def handle(self, args, config, plugin_registry: PluginRegistry):
        """Entrypoint for the inspect command"""
        command_utilities.setup_logging(args.log_level or "warning", config)
        bus_module, bus = command_utilities.import_bus(args)
        api_names: List[str]

        block(bus.client.lazy_load_now())

        # Locally registered APIs
        api_names = [api.meta.name for api in bus.client.api_registry.all()]

        # APIs registered to other services on the bus
        for api_name in bus.client.schema.api_names:
            if api_name not in api_names:
                api_names.append(api_name)

        if not args.internal and not args.api:
            # Hide internal APIs if we don't want them
            api_names = [api_name for api_name in api_names if not api_name.startswith("internal.")]

        if args.api and args.api not in api_names:
            sys.stderr.write(
                f"Specified API was not found locally or within the schema on the bus.\n"
                f"Ensure a Lightbus worker is running for this API.\n"
                f"Cannot continue.\n"
            )
            sys.exit(1)

        api_names_to_inspect = []
        for api_name in api_names:
            if not args.api or self.wildcard_match(args.api, api_name):
                api_names_to_inspect.append(api_name)

        if len(api_names_to_inspect) != 1 and args.follow:
            sys.stderr.write(
                f"The --follow option is only available when following a single API.\n"
                f"Please specify the --api option to select a single API to follow.\n"
            )
            sys.exit(1)

        try:
            for api_name in api_names_to_inspect:
                if not args.api or self.wildcard_match(args.api, api_name):
                    logger.debug(f"Inspecting {api_name}")
                    block(self.search_in_api(args, api_name, bus))
                else:
                    logger.debug(f"API {api_name} did not match {args.api}. Skipping")
        except KeyboardInterrupt:
            logger.info("Stopped by user")

    async def search_in_api(self, args, api_name: str, bus: BusPath):
        transport_pool = bus.client.transport_registry.get_event_transport(api_name)
        async with transport_pool as transport:
            async for message in self.get_messages(args, api_name, args.event, transport, bus):
                if self.match_message(args, message):
                    self.output(args, transport, message, bus)

    async def get_messages(
        self,
        args,
        api_name: str,
        event_name: Optional[str],
        transport: EventTransport,
        bus: BusPath,
    ):
        """Yields messages from various sources

        Messages are returned from sources in this order:

         - Any on-disk cache
         - Reading history from the event transport

        """
        CACHE_PATH.mkdir(parents=True, exist_ok=True)
        event_name = event_name or "*"

        # Construct the cache file name
        file_name_hash = sha1((api_name + "\0" + event_name).encode("utf8")).hexdigest()[:8]
        file_name_api = re.sub("[^a-zA-Z0-9_]", "_", api_name)
        file_name_event = event_name.replace("*", "all")

        cache_file_name = f"{file_name_hash}-{file_name_api}-{file_name_event}.json"
        cache_file = CACHE_PATH / cache_file_name

        logger.debug(f"Loading from cache file {cache_file}. Exists: {cache_file.exists()}")

        # Sanity check
        if not cache_file.exists() and args.cache_only:
            sys.stderr.write(
                f"No cache file exists for {api_name}.{event_name}, but --cache-only was"
                " specified\n"
            )
            sys.exit(1)

        def _progress(force=False):
            if force or (cache_yield_count + transport_yield_count) % 1000 == 0:
                logger.debug(
                    f"Yielded {cache_yield_count} from cache and {transport_yield_count} from bus"
                )

        # Start by reading from cache
        cache_yield_count = 0
        transport_yield_count = 0
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
                                "Event transport does not provide message datetimes. Will not load"
                                " from cache."
                            )
                            break
                        start = (
                            max(event_message.datetime, start) if start else event_message.datetime
                        )

                    cache_yield_count += 1
                    _progress()
                    yield event_message

        if args.cache_only:
            return

        def _write_to_cache(f, event_message):
            f.write(json.dumps(transport.serializer(event_message)))
            f.write("\n")
            f.flush()

        # Now get messages from the transport, writing to the cache as we go
        allow_following = True

        if start:
            logger.debug(
                f"Finished reading from cache. Now reading from {start} on {api_name}.{event_name}"
            )

        while True:
            with cache_file.open("a") as f:
                async for event_message in transport.history(
                    api_name=api_name, event_name=event_name, start=start, start_inclusive=False
                ):
                    _write_to_cache(f, event_message)
                    transport_yield_count += 1
                    _progress()
                    yield event_message
                    if hasattr(event_message, "datetime"):
                        start = event_message.datetime
                    else:
                        # Following requires the datetime property on event messages
                        allow_following = False

            if args.follow:
                if allow_following:
                    # We want to keep waiting for new messages, so wait a second then do it again
                    await sleep(1)
                else:
                    logger.warning(
                        "Event transport does not provide message datetimes. Following not"
                        " supported."
                    )
                    break
            else:
                # No waiting for new messages, so break out of the while loop
                break

        _progress(force=True)

    def wildcard_match(self, pattern: str, s: str) -> bool:
        return fnmatch(s, pattern)

    def version_match(self, pattern: str, version: int) -> bool:
        """Does the given version match the given pattern"""
        match = re.match(r"(<|>|<=|>=|!=|=)(\d+)", pattern)
        if not match:
            sys.stderr.write("Invalid version\n")
            sys.exit(1)

        comparator, version_ = match.groups()

        try:
            version_ = int(version_)
        except TypeError:
            sys.stderr.write("Invalid version\n")
            sys.exit(1)

        return self.compare(comparator, left_value=version, right_value=version_)

    def json_match(self, query: str, data: dict) -> bool:
        """Does the given data match the given jsonpath query"""
        match = re.match(r"(.+?)(<|>|<=|>=|!=|=)(.+)", query)
        if not match:
            sys.stderr.write("Invalid json query\n")
            sys.exit(1)

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

    def output(self, args, transport: EventTransport, message: EventMessage, bus: BusPath):
        """Print out the given message"""

        serialized = transport.serializer(message)
        if args.format in ("json", "pretty"):
            if args.format == "pretty":
                dumped = json.dumps(serialized, indent=4)
            else:
                dumped = json.dumps(serialized)
            sys.stdout.write(dumped)
            sys.stdout.write("\n")
            sys.stdout.flush()

        elif args.format == "human":
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
                    v = json.dumps(v, indent=4)
                    pad = " " * 24
                    v = "".join(pad + v for v in v.splitlines(keepends=True)).lstrip()
                print(f"    {str(k).ljust(20)}: {v}")

            if args.validate or args.show_casting:
                print(f"\n{Colors.BWhite}Extra:{Colors.Reset}")

            if args.validate:
                try:
                    bus.client.schema.validate_parameters(
                        message.api_name, message.event_name, message.kwargs
                    )
                except ValidationError as e:
                    validation_message = f"{Colors.Red}{e}{Colors.Reset}"
                else:
                    validation_message = f"{Colors.Green}Passed{Colors.Reset}"

                print(f"    Validation:         {validation_message}")

            if args.show_casting:
                block(bus.client.hook_registry.execute("before_worker_start"))

                for listener in bus.client.event_client._event_listeners:
                    if (message.api_name, message.event_name) not in listener.events:
                        continue

                    hints = get_type_hints(listener.callable)
                    casted = cast_to_signature(
                        parameters=message.kwargs, callable=listener.callable
                    )
                    print(
                        f"\n    {Colors.BWhite}Casting for listener: {listener.name}{Colors.Reset}"
                    )

                    for key, value in message.kwargs.items():
                        was = type(value)
                        via = hints[key]
                        now = type(casted[key])
                        color = Colors.Green if via == now else Colors.Red
                        print(
                            "        "
                            f"{color}{str(key).ljust(20)}: "
                            f"Received a '{was.__name__}', "
                            f"casted to a '{via.__name__}', "
                            f"result was a '{now.__name__}'"
                            f"{Colors.Reset}"
                        )

            print("\n")

        else:
            sys.stderr.write(f"Unknown output format '{args.format}'\n")
            sys.exit(1)

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
            sys.stderr.write(f"Unknown comparator '{comparator}'\n")
            sys.exit(1)

        return lookup[comparator]()


class Colors:
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
