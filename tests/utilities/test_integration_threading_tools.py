import asyncio
import threading
from unittest import mock

import pytest

from lightbus import BusPath
from lightbus.exceptions import UnknownApi
from lightbus.utilities.async_tools import block, cancel

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_run_in_main_thread_already_in_main_thread(dummy_bus: BusPath, mocker):
    mocker.spy(dummy_bus.client._call_queue.sync_q, "put")
    await dummy_bus.client.listen_for_event("foo", "bar", lambda x: None, listener_name="foo")
    assert not dummy_bus.client._call_queue.sync_q.put.called


@pytest.mark.asyncio
async def test_run_in_main_thread_in_child_thread(dummy_bus: BusPath, mocker):
    def in_thread():
        dummy_bus.foo.bar.listen(lambda x: None, listener_name="foo")

    mocker.spy(dummy_bus.client._call_queue.sync_q, "put")

    perform_calls_task = asyncio.ensure_future(dummy_bus.client._perform_calls())
    await asyncio.get_event_loop().run_in_executor(executor=None, func=in_thread)

    assert dummy_bus.client._call_queue.sync_q.put.called

    await cancel(perform_calls_task)


@pytest.mark.asyncio
async def test_run_in_main_thread_lightbus_error(dummy_bus: BusPath, mocker):
    def in_thread():
        # API not known, so lightbus will raise an error
        dummy_bus.foo.bar.fire()

    mocker.spy(dummy_bus.client._call_queue.sync_q, "put")

    perform_calls_task = asyncio.ensure_future(dummy_bus.client._perform_calls())
    with pytest.raises(UnknownApi):
        await asyncio.get_event_loop().run_in_executor(executor=None, func=in_thread)

    assert dummy_bus.client._call_queue.sync_q.put.called

    await cancel(perform_calls_task)


@pytest.mark.asyncio
async def test_run_in_main_thread_warning_regarding_using_returned_future(
    dummy_bus: BusPath, mocker, caplog
):
    def in_thread():
        task = dummy_bus.foo.bar.listen(lambda x: None, listener_name="foo")
        task.done()  # Attribute access triggers warning

    perform_calls_task = asyncio.ensure_future(dummy_bus.client._perform_calls())
    await asyncio.get_event_loop().run_in_executor(executor=None, func=in_thread)

    log_levels = {r.levelname for r in caplog.records}

    # We show a nice big warning about not using futures/tasks returned from other threads
    assert "WARNING" in log_levels
    await cancel(perform_calls_task)
