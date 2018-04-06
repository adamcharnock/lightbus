import argparse
import logging

from lightbus import create
from lightbus.commands.utilities import BusImportMixin
from lightbus.utilities.importing import import_from_string

logger = logging.getLogger(__name__)


class Command(BusImportMixin, object):

    def setup(self, parser, subparsers):
        parser_run = subparsers.add_parser('run', help='Run Lightbus', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        self.setup_import_parameter(parser_run)

        parser_run_action_group = parser_run.add_mutually_exclusive_group()
        parser_run_action_group.add_argument('--events-only', '-E',
                                             help='Only listen for and handle events, do not respond to RPC calls',
                                             action='store_true')
        parser_run_action_group.add_argument(
            '--schema', '-s',
            help='Manually load the schema from the given file or directory. '
                 'This will normally be provided by the schema transport, '
                 'but manual loading may be useful during development or testing.',
            metavar='FILE_OR_DIRECTORY',
        )
        parser_run.set_defaults(func=self.handle)

        parser_run_transport_group = parser_run.add_argument_group(title='Transport options')
        parser_run_transport_group.add_argument(
            '--rpc-transport', '-p', help='Default RPC transport class to use', default='lightbus.RedisRpcTransport'
        )
        parser_run_transport_group.add_argument(
            '--result-transport', '-t', help='Default result transport class to use', default='lightbus.RedisResultTransport'
        )
        parser_run_transport_group.add_argument(
            '--event-transport', '-e', help='Default event transport class to use', default='lightbus.RedisEventTransport'
        )
        parser_run_transport_group.add_argument(
            '--schema-transport', '-a', help='Default schema transport class to use', default='lightbus.RedisSchemaTransport'
        )

        parser_run_connection_group = parser_run.add_argument_group(title='Connection options')
        parser_run_connection_group.add_argument(
            '--redis-url', '-r', help='URL to Redis server when using Redis-based transports', default='redis://localhost:6379/0'
        )

    def handle(self, args, dry_run=False):
        try:
            rpc_transport = import_from_string(args.rpc_transport)
            result_transport = import_from_string(args.result_transport)
            event_transport = import_from_string(args.event_transport)
            schema_transport = import_from_string(args.schema_transport)
        except ImportError as e:
            logger.critical("Error when trying to import transports: {}. Perhaps check your config for typos.".format(e))
            return

        bus_module = self.import_bus(args)

        bus = create(
            rpc_transport=rpc_transport(url=args.redis_url),
            result_transport=result_transport(url=args.redis_url),
            event_transport=event_transport(url=args.redis_url),
            schema_transport=schema_transport(url=args.redis_url),
        )

        if args.schema:
            if args.schema == '-':
                # if '-' read from stdin
                source = None
            else:
                source = args.schema
            bus.schema.load_local(source)

        before_server_start = getattr(bus_module, 'before_server_start', None)
        if before_server_start:
            logger.debug('Calling {}.before_server_start() callback'.format(bus_module.__name__))
            before_server_start(bus)

        if dry_run:
            return

        if args.events_only:
            bus.run_forever(consume_rpcs=False)
        else:
            bus.run_forever()
