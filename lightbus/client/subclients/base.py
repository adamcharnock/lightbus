import asyncio

from lightbus.schema import Schema
from lightbus.api import ApiRegistry
from lightbus.config import Config
from lightbus.transports.base import TransportRegistry


class BaseSubClient:
    def __init__(
        self,
        transport_registry: TransportRegistry,
        api_registry: ApiRegistry,
        config: Config,
        schema: Schema,
        fatal_errors: asyncio.Queue,
    ):
        self.transport_registry = transport_registry
        self.api_registry = api_registry
        self.config = config
        self.schema = schema
        self.fatal_errors = fatal_errors

    async def lazy_load_now(self):
        raise NotImplementedError()
