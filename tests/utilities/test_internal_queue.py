from queue import Full

import pytest

from lightbus.utilities.internal_queue import InternalQueue


@pytest.mark.asyncio
async def test_internal_queue_put():
    queue = InternalQueue()
    await queue.put(True)


@pytest.mark.asyncio
async def test_internal_queue_put_full_timeout():
    queue = InternalQueue(maxsize=1)
    await queue.put(True)
    with pytest.raises(Full):
        await queue.put(True, timeout=0.1)


@pytest.mark.asyncio
async def test_internal_queue_put_nowait():
    queue = InternalQueue()
    queue.put_nowait(True)


@pytest.mark.asyncio
async def test_internal_queue_put_nowait_full():
    queue = InternalQueue(maxsize=1)
    queue.put_nowait(True)
    with pytest.raises(Full):
        queue.put_nowait(True)


@pytest.mark.asyncio
async def test_internal_queue_get():
    queue = InternalQueue()
    await queue.put(True)
    assert await queue.get() is True
