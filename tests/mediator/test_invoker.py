import asyncio
import logging
from unittest import mock

import pytest
from _pytest.logging import LogCaptureFixture

from lightbus.client.commands import SendEventCommand
from lightbus.client.internal_messaging.producer import InternalProducer
from lightbus.utilities.async_tools import cancel

pytestmark = pytest.mark.unit


@pytest.yield_fixture
async def invoker():
    def _on_exception(e):
        raise e

    invoker = InternalProducer(queue=asyncio.Queue(), error_queue=asyncio.Queue())
    yield invoker
    await invoker.close()


@pytest.fixture
async def fake_coroutine():
    async def fake_coroutine_(*args, **kwargs):
        pass

    return fake_coroutine_


@pytest.mark.asyncio
async def test_set_handler_and_start(invoker: InternalProducer, fake_coroutine):
    invoker.start(fake_coroutine)
    await invoker.wait_until_ready()

    assert not invoker._task.done()
    assert not invoker._queue_monitor_task.done()

    # Ensure the tasks dont explode shortly after they confirm they are ready
    await asyncio.sleep(0.01)
    assert not invoker._task.done()
    assert not invoker._queue_monitor_task.done()


@pytest.mark.asyncio
async def test_queue_monitor(invoker: InternalProducer, caplog: LogCaptureFixture, fake_coroutine):
    """Ensure the queue monitor logs as we expect

    Note that something we implicitly test for here is that the monitor
    does not log lots of duplicate lines. Rather it only logs when
    something changes.
    """
    invoker.size_warning = 3
    invoker.monitor_interval = 0.01
    caplog.set_level(logging.WARNING)

    # Start the invoker running, but cancel the main task which
    # pops messages off the queue
    invoker.set_handler_and_start(fake_coroutine)
    await cancel(invoker._task)

    # No logging yet
    assert not caplog.records

    # Add a couple of items to the queue (still under size_warning)
    invoker.queue.put_nowait(None)
    invoker.queue.put_nowait(None)
    await asyncio.sleep(0.05)

    # Still no logging yet
    assert not caplog.records

    # One more gets us up to the warning level
    invoker.queue.put_nowait(None)
    await asyncio.sleep(0.05)

    # Now we have logging
    assert len(caplog.records) == 1
    assert (
        caplog.records[0].getMessage()
        == "Queue in InternalProducer now has 3 commands. InternalConsumer is NOT running."
    )
    caplog.clear()  # Clear the log messages

    # Let's check we get another messages when the queue gets bigger again
    invoker.queue.put_nowait(None)
    await asyncio.sleep(0.05)

    assert len(caplog.records) == 1
    assert (
        caplog.records[0].getMessage()
        == "Queue in InternalProducer now has 4 commands. InternalConsumer is NOT running."
    )
    caplog.clear()  # Clear the log messages

    # Now check we get logging when the queue shrinks, but is still above the warning level
    invoker.queue.get_nowait()
    invoker.queue.task_done()
    await asyncio.sleep(0.05)

    assert len(caplog.records) == 1
    assert caplog.records[0].getMessage() == (
        "Queue in InternalProducer has shrunk back down to 3 commands. InternalConsumer is NOT running."
    )
    caplog.clear()  # Clear the log messages

    # Now check we get logging when the queue shrinks to BELOW the warning level
    invoker.queue.get_nowait()
    invoker.queue.task_done()
    await asyncio.sleep(0.05)

    assert len(caplog.records) == 1
    assert caplog.records[0].getMessage() == (
        "Queue in InternalProducer has shrunk back down to 2 commands. "
        "InternalConsumer is NOT running. Queue is now at an OK size again."
    )
    caplog.clear()  # Clear the log messages


@pytest.mark.asyncio
async def test_consume_in_parallel(invoker: InternalProducer):
    call_history = []

    # Our handler to track the order in which things were called
    async def fn(command):
        call_history.append(f"Started")
        await asyncio.sleep(0.01)
        call_history.append(f"Finished")

    # Setup our handler and wait for the invoker to be ready
    invoker.set_handler_and_start(fn)
    await invoker.wait_until_ready()

    # Add some things in the queue
    invoker.queue.put_nowait(None)
    invoker.queue.put_nowait(None)

    # Wait until all the calls are done
    for _ in range(0, 10):
        await asyncio.sleep(0.1)
        if len(call_history) == 4:
            break

    # Ensure they were executed in parallel, not sequentially
    assert call_history == ["Started", "Started", "Finished", "Finished"]

    assert len(invoker._running_tasks) == 0


@pytest.mark.asyncio
async def test_consume_on_done(invoker: InternalProducer):
    calls = 0

    async def fn(command):
        nonlocal calls
        calls += 1

    # Setup our handler and wait for the invoker to be ready
    invoker.set_handler_and_start(fn)
    await invoker.wait_until_ready()

    # Add some commands
    command1 = SendEventCommand(message=None, on_done=asyncio.Event())
    command2 = SendEventCommand(message=None, on_done=asyncio.Event())
    invoker.queue.put_nowait(command1)
    invoker.queue.put_nowait(command2)

    await asyncio.sleep(0.05)

    assert command1.on_done.is_set()
    assert command2.on_done.is_set()

    assert calls == 2
    assert len(invoker._running_tasks) == 0
    assert invoker.queue.qsize() == 0


@pytest.mark.asyncio
async def test_consume_not_coroutine(invoker: InternalProducer):
    def fn(command):
        pass

    with pytest.raises(RuntimeError):
        invoker.set_handler_and_start(fn)


@pytest.mark.asyncio
async def test_consume_exception(invoker: InternalProducer):
    exceptions = []

    async def fn(command):
        raise ValueError("Something went wrong")

    def on_exception(e):
        exceptions.append(e)

    # Set out customer exception handler
    invoker._on_exception = on_exception
    # Setup our command handler
    invoker.set_handler_and_start(fn)
    # wait for the invoker to be ready
    await invoker.wait_until_ready()

    from lightbus.client.internal_messaging import invoker as invoker_module

    with mock.patch.object(invoker_module.sys, "exit") as mock_exit:
        command1 = SendEventCommand(message=None, on_done=asyncio.Event())
        invoker.queue.put_nowait(command1)

        # Wait for them to be handled
        await asyncio.sleep(0.05)

    assert mock_exit.call_args[0][0] == 100
