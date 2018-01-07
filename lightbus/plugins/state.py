"""Plugin to broadcast Lightbus' state on the internal.state API"""
import asyncio

from datetime import datetime

from lightbus import BusClient
from lightbus.api import registry
from lightbus.message import EventMessage
from lightbus.plugins import LightbusPlugin, is_plugin_loaded
from lightbus.plugins.metrics import MetricsPlugin
from lightbus.utilities import handle_aio_exceptions

DEFAULT_PING_INTERVAL = 5


class StatePlugin(LightbusPlugin):
    priority = 100

    async def before_server_start(self, *, bus_client: BusClient, loop):
        await bus_client.event_transport.send_event(
            EventMessage(
                api_name='internal.state',
                event_name='server_started',
                kwargs=self.get_state_kwargs(bus_client)
            )
        )
        asyncio.ensure_future(handle_aio_exceptions(self._send_ping(bus_client)), loop=loop)

    async def after_server_stopped(self, *, bus_client: BusClient, loop):
        await bus_client.event_transport.send_event(
            EventMessage(api_name='internal.state', event_name='server_stopped', kwargs=dict(
                process_name='foo',
            ))
        )

    async def _send_ping(self, bus_client: BusClient):
        while True:
            await asyncio.sleep(DEFAULT_PING_INTERVAL)
            await bus_client.event_transport.send_event(
                EventMessage(
                    api_name='internal.state',
                    event_name='server_ping',
                    kwargs=self.get_state_kwargs(bus_client)
                )
            )

    def get_state_kwargs(self, bus_client: BusClient):
        """Get the kwargs for a server_started or ping message"""
        return dict(
            process_name='foo',
            metrics_enabled=is_plugin_loaded(MetricsPlugin),
            api_names=[api.meta.name for api in registry.public()],
            listening_for=['{}.{}'.format(api_name, event_name) for api_name, event_name in bus_client._listeners.keys()],
            timestamp=datetime.utcnow().timestamp(),
        )

