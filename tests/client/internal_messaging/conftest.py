import asyncio

import pytest

from lightbus.client.internal_messaging.consumer import InternalConsumer
from lightbus.client.internal_messaging.producer import InternalProducer


@pytest.yield_fixture
async def consumer():
    def _on_exception(e):
        raise e

    consumer = InternalConsumer(queue=asyncio.Queue(), error_queue=asyncio.Queue())
    yield consumer
    await consumer.close()


@pytest.yield_fixture
async def producer():
    def _on_exception(e):
        raise e

    producer = InternalProducer(queue=asyncio.Queue(), error_queue=asyncio.Queue())
    yield producer
    await producer.close()


@pytest.fixture
async def fake_coroutine():
    async def fake_coroutine_(*args, **kwargs):
        pass

    return fake_coroutine_
