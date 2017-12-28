"""Plugin to broadcast Lightbus' state on the internal.state API"""
import asyncio

from datetime import datetime

from lightbus import BusClient
from lightbus.api import registry
from lightbus.message import EventMessage
from lightbus.plugins import LightbusPlugin
from lightbus.utilities import handle_aio_exceptions, block


class StatePlugin(LightbusPlugin):
    priority = 100

    def server_started(self, *, bus_client: BusClient, loop):
        asyncio.ensure_future(handle_aio_exceptions(bus_client.event_transport.send_event(
            EventMessage(api_name='internal.state', event_name='server_started', kwargs=dict(
                process_name='foo',
                metrics_enabled=True,
                api_names=[api.meta.name for api in registry.public()],
                listening_for=['{}.{}'.format(api_name, event_name) for api_name, event_name in bus_client._listeners.keys()],
                timestamp=datetime.utcnow().timestamp(),
            ))
        )), loop=loop)

    def server_stopped(self, *, bus_client: BusClient, loop):
        block(bus_client.event_transport.send_event(
            EventMessage(api_name='internal.state', event_name='server_stopped', kwargs=dict(
                process_name='foo',
            ))
        ), timeout=0.5)
