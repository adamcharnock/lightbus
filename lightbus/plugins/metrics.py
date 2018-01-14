"""Plugin to broadcast Lightbus' state on the internal.state API"""
import asyncio

from datetime import datetime
from typing import Coroutine, Any

import lightbus
from lightbus import BusClient
from lightbus.api import registry
from lightbus.message import EventMessage, RpcMessage, ResultMessage
from lightbus.plugins import LightbusPlugin, is_plugin_loaded
from lightbus.utilities import handle_aio_exceptions, block


class MetricsPlugin(LightbusPlugin):
    priority = 110

    # Client-side RPC hooks

    async def before_rpc_call(self, *, rpc_message: RpcMessage, bus_client: 'lightbus.bus.BusClient'):
        await self.send_event(bus_client, 'rpc_call_sent',
                              rpc_id=rpc_message.rpc_id,
                              api_name=rpc_message.api_name,
                              procedure_name=rpc_message.procedure_name,
                              kwargs=rpc_message.kwargs,
                              )

    async def after_rpc_call(self, *, rpc_message: RpcMessage, result_message: ResultMessage, bus_client: 'lightbus.bus.BusClient'):
        await self.send_event(bus_client, 'rpc_response_received',
                              rpc_id=rpc_message.rpc_id,
                              api_name=rpc_message.api_name,
                              procedure_name=rpc_message.procedure_name,
                              )

    # Server-side RPC hooks

    async def before_rpc_execution(self, *, rpc_message: RpcMessage, bus_client: 'lightbus.bus.BusClient'):
        await self.send_event(bus_client, 'rpc_call_received',
                              rpc_id=rpc_message.rpc_id,
                              api_name=rpc_message.api_name,
                              procedure_name=rpc_message.procedure_name,
                              )

    async def after_rpc_execution(self, *, rpc_message: RpcMessage, result_message: ResultMessage, bus_client: 'lightbus.bus.BusClient'):
        await self.send_event(bus_client, 'rpc_response_sent',
                              rpc_id=rpc_message.rpc_id,
                              api_name=rpc_message.api_name,
                              procedure_name=rpc_message.procedure_name,
                              result=result_message.result,
                              )

    # Client-side event hooks

    async def after_event_sent(self, *, event_message: EventMessage, bus_client: 'lightbus.bus.BusClient'):
        await self.send_event(bus_client, 'event_fired',
                              event_id='event_id',
                              api_name=event_message.api_name,
                              event_name=event_message.event_name,
                              kwargs=event_message.kwargs,
                              )

    # Server-side event hooks

    async def before_event_execution(self, *, event_message: EventMessage, bus_client: 'lightbus.bus.BusClient'):
        await self.send_event(bus_client, 'event_received',
                              event_id='event_id',
                              api_name=event_message.api_name,
                              event_name=event_message.event_name,
                              kwargs=event_message.kwargs,
                              )

    async def after_event_execution(self, *, event_message: EventMessage, bus_client: 'lightbus.bus.BusClient'):
        await self.send_event(bus_client, 'event_processed',
                              event_id='event_id',
                              api_name=event_message.api_name,
                              event_name=event_message.event_name,
                              kwargs=event_message.kwargs,
                              )

    def send_event(self, bus_client, event_name_, **kwargs) -> Coroutine:
        """Send an event to the bus

        Note that we bypass using BusClient directly, otherwise we would trigger this
        plugin again thereby causing an infinite loop.
        """
        kwargs.setdefault('timestamp', datetime.utcnow().timestamp())
        kwargs.setdefault('process_name', bus_client.process_name)
        return bus_client.event_transport.send_event(
            EventMessage(
                api_name='internal.metrics',
                event_name=event_name_,
                kwargs=kwargs
            ),
            options={},
        )
