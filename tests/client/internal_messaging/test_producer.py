import asyncio
import logging

import pytest
from _pytest.logging import LogCaptureFixture

from lightbus.client.internal_messaging.producer import InternalProducer

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_queue_monitor(producer: InternalProducer, caplog: LogCaptureFixture, fake_coroutine):
    """Ensure the queue monitor logs as we expect

    Note that something we implicitly test for here is that the monitor
    does not log lots of duplicate lines. Rather it only logs when
    something changes.
    """
    producer.size_warning = 3
    producer.monitor_interval = 0.01
    caplog.set_level(logging.WARNING)

    # Start the producer running
    producer.start()

    # No logging yet
    assert not caplog.records

    # Add a couple of items to the queue (still under size_warning)
    producer.queue.put_nowait(None)
    producer.queue.put_nowait(None)
    await asyncio.sleep(0.05)

    # Still no logging yet
    assert not caplog.records

    # One more gets us up to the warning level
    producer.queue.put_nowait(None)
    await asyncio.sleep(0.05)

    # Now we have logging
    assert len(caplog.records) == 1
    assert caplog.records[0].getMessage() == "Queue in InternalProducer now has 3 commands."
    caplog.clear()  # Clear the log messages

    # Let's check we get another messages when the queue gets bigger again
    producer.queue.put_nowait(None)
    await asyncio.sleep(0.05)

    assert len(caplog.records) == 1
    assert caplog.records[0].getMessage() == "Queue in InternalProducer now has 4 commands."
    caplog.clear()  # Clear the log messages

    # Now check we get logging when the queue shrinks, but is still above the warning level
    producer.queue.get_nowait()
    producer.queue.task_done()
    await asyncio.sleep(0.05)

    assert len(caplog.records) == 1
    assert caplog.records[0].getMessage() == (
        "Queue in InternalProducer has shrunk back down to 3 commands."
    )
    caplog.clear()  # Clear the log messages

    # Now check we get logging when the queue shrinks to BELOW the warning level
    producer.queue.get_nowait()
    producer.queue.task_done()
    await asyncio.sleep(0.05)

    assert len(caplog.records) == 1
    assert caplog.records[0].getMessage() == (
        "Queue in InternalProducer has shrunk back down to 2 commands. "
        "Queue is now at an OK size again."
    )
    caplog.clear()  # Clear the log messages
