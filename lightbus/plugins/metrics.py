"""Plugin to broadcast Lightbus' state on the internal.state API"""
from datetime import datetime
from typing import Coroutine, TYPE_CHECKING

from lightbus.message import EventMessage, RpcMessage, ResultMessage
from lightbus.plugins import LightbusPlugin
from lightbus.utilities.deforming import deform_to_bus

if TYPE_CHECKING:
    # pylint: disable=unused-import,cyclic-import
    import lightbus
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
        self, *, rpc_message: RpcMessage, client: "lightbus.client.BusClient"
    ):
        await self.send_event(
            client,
            "rpc_call_sent",
            id=rpc_message.id,
            api_name=rpc_message.api_name,
            procedure_name=rpc_message.procedure_name,
            kwargs=deform_to_bus(rpc_message.kwargs),
        )

    async def after_rpc_call(
        self,
        *,
        rpc_message: RpcMessage,
        result_message: ResultMessage,
        client: "lightbus.client.BusClient",
    ):
        await self.send_event(
            client,
            "rpc_response_received",
            id=rpc_message.id,
            api_name=rpc_message.api_name,
            procedure_name=rpc_message.procedure_name,
        )

    # Worker-side RPC hooks

    async def before_rpc_execution(
        self, *, rpc_message: RpcMessage, client: "lightbus.client.BusClient"
    ):
        await self.send_event(
            client,
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
        client: "lightbus.client.BusClient",
    ):
        await self.send_event(
            client,
            "rpc_response_sent",
            id=rpc_message.id,
            api_name=rpc_message.api_name,
            procedure_name=rpc_message.procedure_name,
            result=deform_to_bus(result_message.result),
        )

    # Client-side event hooks

    async def after_event_sent(
        self, *, event_message: EventMessage, client: "lightbus.client.BusClient"
    ):
        if event_message.api_name == "internal.metrics":
            return

        await self.send_event(
            client,
            "event_fired",
            event_id="event_id",
            api_name=event_message.api_name,
            event_name=event_message.event_name,
            kwargs=deform_to_bus(event_message.kwargs),
        )

    # Worker-side event hooks

    async def before_event_execution(
        self, *, event_message: EventMessage, client: "lightbus.client.BusClient"
    ):
        if event_message.api_name == "internal.metrics":
            return

        await self.send_event(
            client,
            "event_received",
            event_id="event_id",
            api_name=event_message.api_name,
            event_name=event_message.event_name,
            kwargs=deform_to_bus(event_message.kwargs),
        )

    async def after_event_execution(
        self, *, event_message: EventMessage, client: "lightbus.client.BusClient"
    ):
        if event_message.api_name == "internal.metrics":
            return

        await self.send_event(
            client,
            "event_processed",
            event_id="event_id",
            api_name=event_message.api_name,
            event_name=event_message.event_name,
            kwargs=deform_to_bus(event_message.kwargs),
        )

    def send_event(self, client: "lightbus.client.BusClient", event_name_, **kwargs) -> Coroutine:
        """Send an event to the bus
        """
        kwargs.setdefault("timestamp", datetime.utcnow().timestamp())
        kwargs.setdefault("service_name", self.service_name)
        kwargs.setdefault("process_name", self.process_name)
        kwargs = deform_to_bus(kwargs)

        return client.fire_event(
            api_name="internal.metrics", name=event_name_, kwargs=kwargs, options={}
        )
