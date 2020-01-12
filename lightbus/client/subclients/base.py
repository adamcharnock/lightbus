import asyncio

from lightbus.client.internal_messaging.consumer import InternalConsumer
from lightbus.client.internal_messaging.producer import InternalProducer
from lightbus.client.utilities import ErrorQueueType
from lightbus.hooks import HookRegistry
from lightbus.schema import Schema
from lightbus.api import ApiRegistry
from lightbus.config import Config
from lightbus.utilities.internal_queue import InternalQueue


class BaseSubClient:
    def __init__(
        self,
        api_registry: ApiRegistry,
        hook_registry: HookRegistry,
        config: Config,
        schema: Schema,
        error_queue: ErrorQueueType,
        consume_from: InternalQueue,
        produce_to: InternalQueue,
    ):
        self.api_registry = api_registry
        self.hook_registry: HookRegistry = hook_registry
        self.config = config
        self.schema = schema
        self.error_queue = error_queue
        self.producer = InternalProducer(queue=produce_to, error_queue=error_queue)
        self.consumer = InternalConsumer(queue=consume_from, error_queue=error_queue)

        self.producer.start()
        self.consumer.start(self.handle)

    async def handle(self, command):
        raise NotImplementedError()

    async def close(self):
        pass
