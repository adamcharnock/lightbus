import argparse

from lightbus.bus import create
from lightbus.utilities import import_from_string, configure_logging


def lightbus_entry_point():
    parser = argparse.ArgumentParser(description='Lightbus management command.')
    subparsers = parser.add_subparsers(help='Commands', dest='subcommand')
    subparsers.required = True

    parser_start = subparsers.add_parser('start', help='Start Lightbus', formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser_start_action_group = parser_start.add_mutually_exclusive_group()
    parser_start_action_group.add_argument('--events-only', help='Start Lightbus, but only listen for and handle events', action='store_true')
    parser_start_action_group.add_argument('--rpcs-only', help='Start Lightbus, but only consume and handle RPCs', action='store_true')
    parser_start.set_defaults(func=command_start)

    parser_start_transport_group = parser_start.add_argument_group(title='Transport options')
    parser_start_transport_group.add_argument(
        '--rpc-transport', help='RPC transport class to use', default='lightbus.RedisRpcTransport'
    )
    parser_start_transport_group.add_argument(
        '--result-transport', help='Result transport class to use', default='lightbus.RedisResultTransport'
    )
    parser_start_transport_group.add_argument(
        '--event-transport', help='Event transport class to use', default='lightbus.RedisEventTransport'
    )

    # parser_start_connection_group = parser_start.add_argument_group(title='Connection options')
    # parser_start_connection_group.add_argument(
    #     '--redis-url', help='URL to Redis server when using Redis-based transports', default='redis://localhost:6379/0'
    # )

    args = parser.parse_args()
    configure_logging()
    args.func(args)


def command_start(args):
    try:
        rpc_transport = import_from_string(args.rpc_transport)
        result_transport = import_from_string(args.result_transport)
        event_transport = import_from_string(args.event_transport)
    except ImportError as e:
        print("Error when trying to import transports: {}. Perhaps check your config for typos.".format(e))
        exit(1)
        return

    bus = create(
        rpc_transport=rpc_transport(),
        result_transport=result_transport(),
        event_transport=event_transport(),
    )
    if args.events_only:
        bus.run_forever(consume_rpcs=False)
    elif args.rpcs_only:
        bus.run_forever(consume_events=False)
    else:
        bus.run_forever()
