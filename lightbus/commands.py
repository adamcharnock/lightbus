import argparse
import importlib.util

import logging

import sys

from lightbus.bus import create
from lightbus.utilities import import_from_string, configure_logging, autodiscover

logger = logging.getLogger(__name__)


def lightbus_entry_point():  # pragma: no cover
    configure_logging()
    args = parse_args()
    args.func(args)


def parse_args(args=None):
    parser = argparse.ArgumentParser(description='Lightbus management command.')
    subparsers = parser.add_subparsers(help='Commands', dest='subcommand')
    subparsers.required = True

    parser_run = subparsers.add_parser('run', help='Run Lightbus', formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser_run_action_group = parser_run.add_mutually_exclusive_group()
    parser_run_action_group.add_argument('--events-only', help='Run Lightbus, but only listen for and handle events', action='store_true')
    parser_run_action_group.add_argument('--rpcs-only', help='Run Lightbus, but only consume and handle RPCs', action='store_true')
    parser_run_action_group.add_argument('--import', dest='imprt', help='The Python module to import initially. If omited ')
    parser_run.set_defaults(func=command_run)

    parser_run_transport_group = parser_run.add_argument_group(title='Transport options')
    parser_run_transport_group.add_argument(
        '--rpc-transport', help='RPC transport class to use', default='lightbus.RedisRpcTransport'
    )
    parser_run_transport_group.add_argument(
        '--result-transport', help='Result transport class to use', default='lightbus.RedisResultTransport'
    )
    parser_run_transport_group.add_argument(
        '--event-transport', help='Event transport class to use', default='lightbus.RedisEventTransport'
    )

    # parser_run_connection_group = parser_run.add_argument_group(title='Connection options')
    # parser_run_connection_group.add_argument(
    #     '--redis-url', help='URL to Redis server when using Redis-based transports', default='redis://localhost:6379/0'
    # )

    return parser.parse_args(sys.argv if args is None else args)


def command_run(args, dry_run=False):
    try:
        rpc_transport = import_from_string(args.rpc_transport)
        result_transport = import_from_string(args.result_transport)
        event_transport = import_from_string(args.event_transport)
    except ImportError as e:
        logger.critical("Error when trying to import transports: {}. Perhaps check your config for typos.".format(e))
        return

    if args.imprt:
        spec = importlib.util.find_spec(args.imprt)
        if not spec:
            logger.critical("Could not find module '{}' as specified by --import. Ensure "
                            "this module is available on your PYTHONPATH.".format(args.imprt))
            return
        bus_module = importlib.util.module_from_spec(spec)
    else:
        bus_module = autodiscover()

    if bus_module is None:
        logger.warning('Could not find a bus.py file, will listen for events only.')

    bus = create(
        rpc_transport=rpc_transport(),
        result_transport=result_transport(),
        event_transport=event_transport(),
    )

    before_server_start = getattr(bus_module, 'before_server_start', None)
    if before_server_start:
        logger.debug('Calling {}.before_server_start() callback'.format(bus_module.__name__))
        before_server_start(bus)

    if dry_run:
        return

    if args.events_only:
        bus.run_forever(consume_rpcs=False)
    elif args.rpcs_only:
        bus.run_forever(consume_events=False)
    else:
        bus.run_forever()
