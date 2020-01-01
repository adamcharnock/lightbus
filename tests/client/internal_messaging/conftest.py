import asyncio

import pytest

from lightbus.client.internal_messaging.consumer import InternalConsumer
from lightbus.client.internal_messaging.producer import InternalProducer
from lightbus.utilities.internal_queue import InternalQueue


@pytest.yield_fixture
async def consumer():
    def _on_exception(e):
        raise e

    # TODO: Close queue
    consumer = InternalConsumer(queue=InternalQueue(), error_queue=InternalQueue())
    yield consumer
    await consumer.close()


@pytest.yield_fixture
async def producer():
    def _on_exception(e):
        raise e

    # TODO: Close queue
    producer = InternalProducer(queue=InternalQueue(), error_queue=InternalQueue())
    yield producer
    await producer.close()


@pytest.fixture
async def fake_coroutine():
    async def fake_coroutine_(*args, **kwargs):
        pass

    return fake_coroutine_
