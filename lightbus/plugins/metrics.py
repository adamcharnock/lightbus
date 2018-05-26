"""Plugin to broadcast Lightbus' state on the internal.state API"""
from datetime import datetime
from typing import Coroutine

import lightbus
from lightbus.message import EventMessage, RpcMessage, ResultMessage
from lightbus.plugins import LightbusPlugin

if False:
    from lightbus.config import Config


class MetricsPlugin(LightbusPlugin):
    priority = 110

    def __init__(self, service_name: str, process_name: str):
        self.service_name = service_name
        self.process_name = process_name

    @classmethod
    def from_config(cls, config: "Config"):
        return cls(service_name=config.service_name, process_name=config.process_name)

    # Client-side RPC hooks

    async def before_rpc_call(
        self, *, rpc_message: RpcMessage, bus_client: "lightbus.bus.BusClient"
    ):
        await self.send_event(
            bus_client,
            "rpc_call_sent",
            id=rpc_message.id,
            api_name=rpc_message.api_name,
            procedure_name=rpc_message.procedure_name,
            kwargs=rpc_message.kwargs,
        )

    async def after_rpc_call(
        self,
        *,
        rpc_message: RpcMessage,
        result_message: ResultMessage,
        bus_client: "lightbus.bus.BusClient",
    ):
        await self.send_event(
            bus_client,
            "rpc_response_received",
            id=rpc_message.id,
            api_name=rpc_message.api_name,
            procedure_name=rpc_message.procedure_name,
        )

    # Server-side RPC hooks

    async def before_rpc_execution(
        self, *, rpc_message: RpcMessage, bus_client: "lightbus.bus.BusClient"
    ):
        await self.send_event(
            bus_client,
            "rpc_call_received",
            id=rpc_message.id,
            api_name=rpc_message.api_name,
            procedure_name=rpc_message.procedure_name,
        )

    async def after_rpc_execution(
        self,
        *,
        rpc_message: RpcMessage,
        result_message: ResultMessage,
        bus_client: "lightbus.bus.BusClient",
    ):
        await self.send_event(
            bus_client,
            "rpc_response_sent",
            id=rpc_message.id,
            api_name=rpc_message.api_name,
            procedure_name=rpc_message.procedure_name,
            result=result_message.result,
        )

    # Client-side event hooks

    async def after_event_sent(
        self, *, event_message: EventMessage, bus_client: "lightbus.bus.BusClient"
    ):
        await self.send_event(
            bus_client,
            "event_fired",
            event_id="event_id",
            api_name=event_message.api_name,
            event_name=event_message.event_name,
            kwargs=event_message.kwargs,
        )

    # Server-side event hooks

    async def before_event_execution(
        self, *, event_message: EventMessage, bus_client: "lightbus.bus.BusClient"
    ):
        await self.send_event(
            bus_client,
            "event_received",
            event_id="event_id",
            api_name=event_message.api_name,
            event_name=event_message.event_name,
            kwargs=event_message.kwargs,
        )

    async def after_event_execution(
        self, *, event_message: EventMessage, bus_client: "lightbus.bus.BusClient"
    ):
        await self.send_event(
            bus_client,
            "event_processed",
            event_id="event_id",
            api_name=event_message.api_name,
            event_name=event_message.event_name,
            kwargs=event_message.kwargs,
        )

    def send_event(self, bus_client, event_name_, **kwargs) -> Coroutine:
        """Send an event to the bus

        Note that we bypass using BusClient directly, otherwise we would trigger this
        plugin again thereby causing an infinite loop.
        """
        kwargs.setdefault("timestamp", datetime.utcnow().timestamp())
        kwargs.setdefault("service_name", self.service_name)
        kwargs.setdefault("process_name", self.process_name)
        event_transport = bus_client.transport_registry.get_event_transport("internal.metrics")
        return event_transport.send_event(
            EventMessage(api_name="internal.metrics", event_name=event_name_, kwargs=kwargs),
            options={},
        )
