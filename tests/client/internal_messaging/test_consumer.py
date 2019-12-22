import asyncio
from unittest import mock

import pytest

from lightbus.client.commands import SendEventCommand
from lightbus.client.internal_messaging.consumer import InternalConsumer


@pytest.mark.asyncio
async def test_set_handler_and_start(consumer: InternalConsumer, fake_coroutine):
    consumer.start(fake_coroutine)
    await consumer.wait_until_ready()

    assert not consumer._task.done()
    assert not consumer._queue_monitor_task.done()

    # Ensure the tasks dont explode shortly after they confirm they are ready
    await asyncio.sleep(0.01)
    assert not consumer._task.done()
    assert not consumer._queue_monitor_task.done()


@pytest.mark.asyncio
async def test_consume_in_parallel(consumer: InternalConsumer):
    call_history = []

    # Our handler to track the order in which things were called
    async def fn(command):
        call_history.append(f"Started")
        await asyncio.sleep(0.01)
        call_history.append(f"Finished")

    # Setup our handler and wait for the consumer to be ready
    consumer.set_handler_and_start(fn)
    await consumer.wait_until_ready()

    # Add some things in the queue
    consumer.queue.put_nowait(None)
    consumer.queue.put_nowait(None)

    # Wait until all the calls are done
    for _ in range(0, 10):
        await asyncio.sleep(0.1)
        if len(call_history) == 4:
            break

    # Ensure they were executed in parallel, not sequentially
    assert call_history == ["Started", "Started", "Finished", "Finished"]

    assert len(consumer._running_tasks) == 0


@pytest.mark.asyncio
async def test_consume_on_done(consumer: InternalConsumer):
    calls = 0

    async def fn(command):
        nonlocal calls
        calls += 1

    # Setup our handler and wait for the consumer to be ready
    consumer.set_handler_and_start(fn)
    await consumer.wait_until_ready()

    # Add some commands
    command1 = SendEventCommand(message=None, on_done=asyncio.Event())
    command2 = SendEventCommand(message=None, on_done=asyncio.Event())
    consumer.queue.put_nowait(command1)
    consumer.queue.put_nowait(command2)

    await asyncio.sleep(0.05)

    assert command1.on_done.is_set()
    assert command2.on_done.is_set()

    assert calls == 2
    assert len(consumer._running_tasks) == 0
    assert consumer.queue.qsize() == 0


@pytest.mark.asyncio
async def test_consume_not_coroutine(consumer: InternalConsumer):
    def fn(command):
        pass

    with pytest.raises(RuntimeError):
        consumer.set_handler_and_start(fn)


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
    consumer.set_handler_and_start(fn)
    # wait for the consumer to be ready
    await consumer.wait_until_ready()

    from lightbus.client.internal_messaging import consumer as consumer_module

    with mock.patch.object(consumer_module.sys, "exit") as mock_exit:
        command1 = SendEventCommand(message=None, on_done=asyncio.Event())
        consumer.queue.put_nowait(command1)

        # Wait for them to be handled
        await asyncio.sleep(0.05)

    assert mock_exit.call_args[0][0] == 100
