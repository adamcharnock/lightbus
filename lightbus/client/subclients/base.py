import asyncio

from lightbus.client.internal_messaging.consumer import InternalConsumer
from lightbus.client.internal_messaging.producer import InternalProducer
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
        error_queue: asyncio.Queue,
        consume_from: asyncio.Queue,
        produce_to: asyncio.Queue,
    ):
        self.transport_registry = transport_registry
        self.api_registry = api_registry
        self.config = config
        self.schema = schema
        self.error_queue = error_queue
        self.producer = InternalProducer(queue=produce_to, error_queue=error_queue)
        self.consumer = InternalConsumer(queue=consume_from, error_queue=error_queue)

        self.producer.start()
        self.consumer.start(self.handle)

    async def handle(self, command):
        raise NotImplementedError()
