import asyncio
import logging
from asyncio import QueueFull, QueueEmpty, sleep
from functools import partial
from threading import Semaphore, Thread, current_thread

import pytest

from lightbus.utilities.internal_queue import InternalQueue

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_internal_queue_put():
    queue = InternalQueue()
    await queue.put(True)


@pytest.mark.asyncio
async def test_internal_queue_put_nowait():
    queue = InternalQueue()
    queue.put_nowait(True)


@pytest.mark.asyncio
async def test_internal_queue_put_nowait_full():
    queue = InternalQueue(maxsize=1)
    queue.put_nowait(True)
    with pytest.raises(QueueFull):
        queue.put_nowait(True)


@pytest.mark.asyncio
async def test_internal_queue_get():
    queue = InternalQueue()
    await queue.put(True)
    assert await queue.get() is True


@pytest.mark.asyncio
async def test_internal_queue_get_nowait():
    queue = InternalQueue()
    await queue.put(True)
    assert queue.get_nowait() is True


@pytest.mark.asyncio
async def test_internal_queue_get_nowait_empty():
    queue = InternalQueue()
    with pytest.raises(QueueEmpty):
        queue.get_nowait()


@pytest.mark.asyncio
async def test_internal_queue_get_delay():
    queue = InternalQueue()
    task = asyncio.create_task(queue.get())
    await asyncio.sleep(0.001)
    queue.put_nowait(True)
    await asyncio.sleep(0.001)
    assert task.done()
    assert task.result() is True


@pytest.mark.asyncio
async def test_internal_queue_put_delay():
    queue = InternalQueue(maxsize=1)
    queue.put_nowait(False)
    task = asyncio.create_task(queue.put(True))

    await asyncio.sleep(0.001)
    assert queue.get_nowait() is False
    await asyncio.sleep(0.001)
    assert task.done()
    assert queue.get_nowait() is True


@pytest.mark.parametrize(
    "start_order", ["consumer_first", "producer_first"], ids=["consumer_first", "producer_first"]
)
@pytest.mark.parametrize("maxsize", [0, 5], ids=["inf_queue", "small_queue"])
@pytest.mark.parametrize(
    "num_threads", [(20, 20), (20, 5), (5, 20)], ids=["equal", "more_consumers", "more_producers"]
)
def test_internal_thread_safety(start_order, maxsize, num_threads):
    num_consumer_threads, num_producer_threads = num_threads
    queue = InternalQueue(maxsize=maxsize)  # TODO: With max size too
    consumer_counter = Semaphore(value=0)
    producer_counter = Semaphore(value=0)

    total_expected_items = num_producer_threads * 1000
    total_per_consumer = total_expected_items // num_consumer_threads

    async def consumer(q: InternalQueue):
        try:
            for _ in range(0, total_per_consumer):
                await q.get()
                consumer_counter.release()
        except Exception as e:
            logging.exception(e)

    async def producer(q: InternalQueue):
        try:
            for _ in range(0, 1000):
                await q.put(1)
                producer_counter.release()
        except Exception as e:
            logging.exception(e)

    consumer_factory = lambda: partial(asyncio.run, consumer(queue))
    producer_factory = lambda: partial(asyncio.run, producer(queue))

    consumers = [Thread(target=consumer_factory()) for _ in range(0, num_consumer_threads)]
    producers = [Thread(target=producer_factory()) for _ in range(0, num_producer_threads)]

    if start_order == "consumer_first":
        threads = consumers + producers
    else:
        threads = producers + consumers

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    assert consumer_counter._value == total_expected_items
    assert producer_counter._value == total_expected_items
