"""Plugin to broadcast Lightbus' state on the internal.state API"""
import asyncio
import logging
import resource
import socket
from argparse import ArgumentParser, _ArgumentGroup, Namespace
from datetime import datetime

import os
from itertools import chain

from typing import TYPE_CHECKING

from lightbus.message import EventMessage
from lightbus.plugins import LightbusPlugin
from lightbus.plugins.metrics import MetricsPlugin
from lightbus.utilities.async_tools import cancel

if TYPE_CHECKING:
    # pylint: disable=unused-import,cyclic-import
    from lightbus import BusClient
    from lightbus.config import Config

logger = logging.getLogger(__name__)


class StatePlugin(LightbusPlugin):
    """Fire events to the bus regarding the state of this Lightbus worker

    This plugin allows the state of each Lightbus worker to be monitored
    via the bus itself. This is provided by the `internal.state` API.

    This plugin provides coarse monitoring in the following form:

      - Worker started events
      - Worker ping events - indicate the worker is alive. Sent every 60 seconds by default
      - Worker shutdown events

    See `lightbus.internal_apis.LightbusStateApi` for more details on the events provided.

    Note that this plugin provides the basic events required in order for the
    Lightbus admin interface to function. It is therefore suggested that you leave
    it in operation as the traffic generated should be minimal.

    Per-message events are available via the MetricsPlugin, which is substantially higher volume.
    """

    priority = 100

    def __init__(
        self,
        service_name: str,
        process_name: str,
        ping_enabled: bool = True,
        ping_interval: int = 60,
    ):
        self.service_name = service_name
        self.process_name = process_name
        self.ping_enabled = ping_enabled
        self.ping_interval = ping_interval

    @classmethod
    def from_config(cls, config: "Config", ping_enabled: bool = True, ping_interval: int = 60):
        return cls(
            service_name=config.service_name,
            process_name=config.process_name,
            ping_enabled=ping_enabled,
            ping_interval=ping_interval,
        )

    async def before_parse_args(self, *, parser: ArgumentParser, subparsers: _ArgumentGroup):
        """Add some plugin-related args so behaviour can be customised"""
        run_command_parser = subparsers.choices["run"]
        state_run_group = run_command_parser.add_argument_group(title="State plugin arguments")
        state_run_group.add_argument(
            "--ping-interval",
            help="Interval between worker ping events in seconds. Ping events alert the bus "
            "that this Lightbus worker is alive, and are used to update the lightbus admin interface.",
            metavar="SECONDS",
            type=int,
            default=self.ping_interval,
        )
        state_run_group.add_argument(
            "--no-ping",
            help="Disable sending ping events on the internal.state API. This "
            "may result in your worker not appearing in the lightbus admin interface, "
            "but will reduce traffic and log volume.",
            action="store_true",
        )

    async def receive_args(self, args: Namespace):
        if args.subcommand == "run":
            self.ping_enabled = not args.no_ping
            self.ping_interval = args.ping_interval or self.ping_interval

    async def before_worker_start(self, *, client: "BusClient"):
        event_transport = client.transport_registry.get_event_transport("internal.metrics")
        await event_transport.send_event(
            EventMessage(
                api_name="internal.state",
                event_name="worker_started",
                kwargs=self.get_state_kwargs(client),
            ),
            options={},
        )
        if self.ping_enabled:
            logger.info("Ping messages will be sent every {} seconds".format(self.ping_interval))
            self._ping_task = asyncio.ensure_future(self._send_ping(client))
        else:
            logger.warning(
                "Ping events have been disabled. This will reduce log volume and bus traffic, but "
                "may result in this Lightbus worker not appearing in the Lightbus admin interface."
            )

    async def after_worker_stopped(self, *, client: "BusClient"):
        event_transport = client.transport_registry.get_event_transport("internal.metrics")
        await event_transport.send_event(
            EventMessage(
                api_name="internal.state",
                event_name="worker_stopped",
                kwargs=dict(process_name=self.process_name, service_name=self.service_name),
            ),
            options={},
        )
        await cancel(self._ping_task)

    async def _send_ping(self, client: "BusClient"):
        event_transport = client.transport_registry.get_event_transport("internal.metrics")
        while True:
            await asyncio.sleep(self.ping_interval)
            await event_transport.send_event(
                EventMessage(
                    api_name="internal.state",
                    event_name="worker_ping",
                    kwargs=self.get_state_kwargs(client),
                ),
                options={},
            )

    def get_state_kwargs(self, client: "BusClient"):
        """Get the kwargs for a worker_started or ping message"""
        max_memory_use = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        event_listeners = chain(
            *[event_listener.events for event_listener in client.event_client._event_listeners]
        )
        return dict(
            process_name=self.process_name,
            service_name=self.service_name,
            metrics_enabled=client.plugin_registry.is_plugin_loaded(MetricsPlugin),
            api_names=[api.meta.name for api in client.api_registry.public()],
            listening_for=[
                "{}.{}".format(api_name, event_name) for api_name, event_name in event_listeners
            ],
            timestamp=datetime.utcnow().timestamp(),
            ping_enabled=self.ping_enabled,
            ping_interval=self.ping_interval,
            hostname=socket.gethostname(),
            pid=os.getpid(),
            max_memory_use=max_memory_use,
        )
