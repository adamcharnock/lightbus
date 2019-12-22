import asyncio

import pytest

from lightbus.client.commands import SendEventCommand
from lightbus.client.internal_messaging.consumer import InternalConsumer
from lightbus.client.utilities import Error
from lightbus.exceptions import AsyncFunctionOrMethodRequired

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_set_handler_and_start(consumer: InternalConsumer, fake_coroutine):
    consumer.start(fake_coroutine)
    assert not consumer._consumer_task.done()

    # Ensure the tasks dont explode shortly after they confirm they are ready
    await asyncio.sleep(0.01)
    assert not consumer._consumer_task.done()


@pytest.mark.asyncio
async def test_consume_in_parallel(consumer: InternalConsumer):
    call_history = []

    # Our handler to track the order in which things were called
    async def fn(command):
        call_history.append(f"Started")
        await asyncio.sleep(0.01)
        call_history.append(f"Finished")

    # Setup our handler and wait for the consumer to be ready
    consumer.start(fn)

    # Add some things in the queue
    consumer.queue.put_nowait((None, asyncio.Event()))
    consumer.queue.put_nowait((None, asyncio.Event()))

    # Wait until all the calls are done
    for _ in range(0, 10):
        await asyncio.sleep(0.1)
        if len(call_history) == 4:
            break

    # Ensure they were executed in parallel, not sequentially
    assert call_history == ["Started", "Started", "Finished", "Finished"]

    assert len(consumer._running_commands) == 0


@pytest.mark.asyncio
async def test_consume_on_done(consumer: InternalConsumer):
    calls = 0

    async def fn(command):
        nonlocal calls
        calls += 1

    # Setup our handler and wait for the consumer to be ready
    consumer.start(fn)

    # Add some commands
    command1 = SendEventCommand(message=None)
    on_done1 = asyncio.Event()
    command2 = SendEventCommand(message=None)
    on_done2 = asyncio.Event()

    consumer.queue.put_nowait((command1, on_done1))
    consumer.queue.put_nowait((command2, on_done2))

    await asyncio.sleep(0.05)

    assert on_done1.is_set()
    assert on_done2.is_set()

    assert calls == 2
    assert len(consumer._running_commands) == 0
    assert consumer.queue.qsize() == 0


@pytest.mark.asyncio
async def test_consume_not_coroutine(consumer: InternalConsumer):
    def fn(command):
        pass

    with pytest.raises(AsyncFunctionOrMethodRequired):
        consumer.start(fn)


@pytest.mark.asyncio
async def test_consume_exception(consumer: InternalConsumer):
    exceptions = []

    async def fn(command):
        raise ValueError("Something went wrong")

    def on_exception(e):
        exceptions.append(e)

    # Set out customer exception handler
    consumer._on_exception = on_exception
    # Setup our command handler
    consumer.start(fn)

    from lightbus.client.internal_messaging import consumer as consumer_module

    command = SendEventCommand(message=None)
    on_done = asyncio.Event()
    consumer.queue.put_nowait((command, on_done))
    await on_done.wait()

    error: Error = consumer.error_queue.get_nowait()
    assert error.type == ValueError
