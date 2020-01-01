import asyncio

from lightbus.client.internal_messaging.consumer import InternalConsumer
from lightbus.client.internal_messaging.producer import InternalProducer
from lightbus.api import ApiRegistry
from lightbus.client.utilities import ErrorQueueType
from lightbus.config import Config
from lightbus.transports.registry import TransportRegistry
from lightbus.utilities.internal_queue import InternalQueue


class BaseDock:
    """The base dock

    A dock is responsible for interfacing a transport with Lightbus' internal
    messaging system.
    """

    def __init__(
        self,
        transport_registry: TransportRegistry,
        api_registry: ApiRegistry,
        config: Config,
        error_queue: ErrorQueueType,
        consume_from: InternalQueue,
        produce_to: InternalQueue,
    ):
        self.transport_registry = transport_registry
        self.api_registry = api_registry
        self.config = config
        self.error_queue = error_queue
        self.producer = InternalProducer(queue=produce_to, error_queue=error_queue)
        self.consumer = InternalConsumer(queue=consume_from, error_queue=error_queue)

        self.producer.start()
        self.consumer.start(self.handle)

    async def handle(self, command):
        raise NotImplementedError()
