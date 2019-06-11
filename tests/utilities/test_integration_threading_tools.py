import asyncio
import threading
from unittest import mock

import pytest

from lightbus import BusPath
from lightbus.exceptions import UnknownApi
from lightbus.utilities.async_tools import block, cancel
from lightbus.client_worker import run_in_worker_thread

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_run_in_worker_thread_in_child_thread(dummy_bus: BusPath, mocker):
    def in_thread():
        dummy_bus.foo.bar.listen(lambda x: None, listener_name="foo")

    mocker.spy(dummy_bus.client._call_queue.sync_q, "put")

    await asyncio.get_event_loop().run_in_executor(executor=None, func=in_thread)

    assert dummy_bus.client._call_queue.sync_q.put.call_count == 1


@pytest.mark.asyncio
async def test_run_in_worker_thread_lightbus_error(dummy_bus: BusPath, mocker):
    def in_thread():
        # API not known, so lightbus will raise an error
        dummy_bus.foo.bar.fire()

    mocker.spy(dummy_bus.client._call_queue.sync_q, "put")

    with pytest.raises(UnknownApi):
        await asyncio.get_event_loop().run_in_executor(executor=None, func=in_thread)

    assert dummy_bus.client._call_queue.sync_q.put.call_count == 1


@pytest.mark.asyncio
async def test_run_in_worker_thread_warning_regarding_using_returned_future(
    dummy_bus: BusPath, mocker, caplog
):
    async def co():
        return asyncio.Future()

    decorated_fn = run_in_worker_thread(dummy_bus.client)(co)
    fut = await decorated_fn()
    fut.done()  # Attribute access triggers warning

    log_levels = {r.levelname for r in caplog.records}

    # We show a nice big warning about not using futures/tasks returned from other threads
    assert "WARNING" in log_levels
