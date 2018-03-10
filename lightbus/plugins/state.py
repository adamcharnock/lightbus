"""Plugin to broadcast Lightbus' state on the internal.state API"""
import asyncio
import logging
import socket
from argparse import ArgumentParser, _ArgumentGroup, Namespace

from datetime import datetime

import os
import resource

import sys

from lightbus import BusClient
from lightbus.api import registry
from lightbus.message import EventMessage
from lightbus.plugins import LightbusPlugin, is_plugin_loaded
from lightbus.plugins.metrics import MetricsPlugin
from lightbus.utilities import handle_aio_exceptions


logger = logging.getLogger(__name__)


class StatePlugin(LightbusPlugin):
    """Fire events to the bus regarding the state of this Lightbus server

    This plugin allows the state of each Lightbus server to be monitored
    via the bus itself. This is provided by the `internal.state` API.

    This plugin provides coarse monitoring in the following form:

      - Server started events
      - Server ping events - indicate the server is alive. Sent every 60 seconds by default
      - Server shutdown events
      - Metrics enabled/disabled
      - Api registered/deregistered
      - Event listening started/stopped

    See `lightbus.internal_apis.LightbusStateApi` for more details on the events provided.

    Note that this plugin provides the basic events required in order for the
    Lightbus admin interface to function. It is therefore suggested that you leave
    it in operation as the traffic generated should be minimal.

    Per-message events are available via the MetricsPlugin, which is substantially higher volume.
    """
    priority = 100

    def __init__(self):
        self.do_ping = True
        self.ping_interval = 60

    async def before_parse_args(self, *, parser: ArgumentParser, subparsers: _ArgumentGroup):
        """Add some plugin-related args so behaviour can be customised"""
        run_command_parser = subparsers.choices['run']
        state_run_group = run_command_parser.add_argument_group(title='State plugin options')
        state_run_group.add_argument(
            '--ping-interval',
            help='Interval between server ping events in seconds. Ping events alert the bus '
                 'that this Lightbus server is alive, and are used to update the lightbus admin interface.',
            metavar='SECONDS',
            type=int,
            default=self.ping_interval,
        )
        state_run_group.add_argument(
            '--no-ping',
            help='Disable sending ping events on the internal.state API. This '
                 'may result in your server not appearing in the lightbus admin interface, '
                 'but will reduce traffic and log volume.',
            action='store_true',
        )

    async def after_parse_args(self, args: Namespace):
        if args.subcommand == 'run':
            self.do_ping = not args.no_ping
            self.ping_interval = args.ping_interval

    async def before_server_start(self, *, bus_client: BusClient):
        await bus_client.event_transport.send_event(
            EventMessage(
                api_name='internal.state',
                event_name='server_started',
                kwargs=self.get_state_kwargs(bus_client)
            ),
            options={},
        )
        if self.do_ping:
            logger.info(
                'Ping messages will be sent every {} seconds'.format(self.ping_interval)
            )
            asyncio.ensure_future(handle_aio_exceptions(self._send_ping(bus_client)), loop=loop)
        else:
            logger.warning(
                'Ping events have been disabled. This will reduce log volume and bus traffic, but '
                'may result in this Lightbus server not appearing in the Lightbus admin interface.'
            )

    async def after_server_stopped(self, *, bus_client: BusClient):
        await bus_client.event_transport.send_event(
            EventMessage(api_name='internal.state', event_name='server_stopped', kwargs=dict(
                process_name=bus_client.process_name,
            )),
            options={},
        )

    async def _send_ping(self, bus_client: BusClient):
        while True:
            await asyncio.sleep(self.ping_interval)
            await bus_client.event_transport.send_event(
                EventMessage(
                    api_name='internal.state',
                    event_name='server_ping',
                    kwargs=self.get_state_kwargs(bus_client)
                ),
                options={},
            )

    def get_state_kwargs(self, bus_client: BusClient):
        """Get the kwargs for a server_started or ping message"""
        max_memory_use = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return dict(
            process_name=bus_client.process_name,
            metrics_enabled=is_plugin_loaded(MetricsPlugin),
            api_names=[api.meta.name for api in registry.public()],
            listening_for=['{}.{}'.format(api_name, event_name) for api_name, event_name in bus_client._listeners.keys()],
            timestamp=datetime.utcnow().timestamp(),
            ping_enabled=self.do_ping,
            ping_interval=self.ping_interval,
            hostname=socket.gethostname(),
            pid=os.getpid(),
            max_memory_use=max_memory_use
        )

