import asyncio
from datetime import timedelta

import pytest
import schedule

from lightbus.exceptions import CannotBlockHere
from lightbus.utilities.async_tools import (
    call_every,
    cancel,
    call_on_schedule,
    run_user_provided_callable,
    block,
)

pytestmark = pytest.mark.unit


@pytest.fixture()
def call_counter():
    class CallCounter(object):
        def __init__(self):
            self.call_count = 0

        def __call__(self, *args, **kwargs):
            self.call_count += 1

    return CallCounter()


@pytest.mark.asyncio
async def test_block_within_event_loop():
    # Should give an error
    async def co():
        pass

    with pytest.raises(CannotBlockHere):
        block(co())


def test_block_outside_event_loop():
    # Should not raise an error
    async def co():
        return 1

    resuult = block(co())
    assert resuult == 1


@pytest.fixture()
async def run_for():
    async def run_for_inner(coroutine, seconds):
        task = asyncio.ensure_future(coroutine)
        await asyncio.sleep(seconds)
        await cancel(task)

    return run_for_inner


# call_every()


@pytest.mark.asyncio
async def test_call_every(run_for, call_counter):
    await run_for(
        coroutine=call_every(
            callback=call_counter, timedelta=timedelta(seconds=0.1), also_run_immediately=False
        ),
        seconds=0.25,
    )
    assert call_counter.call_count == 2


@pytest.mark.asyncio
async def test_call_every_run_immediate(run_for, call_counter):
    await run_for(
        coroutine=call_every(
            callback=call_counter, timedelta=timedelta(seconds=0.1), also_run_immediately=True
        ),
        seconds=0.25,
    )
    assert call_counter.call_count == 3


@pytest.mark.asyncio
async def test_call_every_async(run_for):
    await_count = 0

    async def cb():
        nonlocal await_count
        await_count += 1

    await run_for(
        coroutine=call_every(
            callback=cb, timedelta=timedelta(seconds=0.1), also_run_immediately=False
        ),
        seconds=0.25,
    )
    assert await_count == 2


@pytest.mark.asyncio
async def test_call_every_with_long_execution_time(run_for):
    """Execution time should get taken into account"""
    await_count = 0

    async def cb():
        nonlocal await_count
        await_count += 1
        await asyncio.sleep(0.09)

    await run_for(
        coroutine=call_every(
            callback=cb, timedelta=timedelta(seconds=0.1), also_run_immediately=False
        ),
        seconds=0.25,
    )
    assert await_count == 2


# call_on_schedule()


@pytest.mark.asyncio
async def test_call_on_schedule(run_for, call_counter):
    await run_for(
        coroutine=call_on_schedule(
            callback=call_counter, schedule=schedule.every(0.1).seconds, also_run_immediately=False
        ),
        seconds=0.25,
    )
    assert call_counter.call_count == 2


@pytest.mark.asyncio
async def test_call_on_schedule_run_immediate(run_for, call_counter):
    await run_for(
        coroutine=call_on_schedule(
            callback=call_counter, schedule=schedule.every(0.1).seconds, also_run_immediately=True
        ),
        seconds=0.25,
    )
    assert call_counter.call_count == 3


@pytest.mark.asyncio
async def test_call_on_schedule_async(run_for):
    import schedule

    await_count = 0

    async def cb():
        nonlocal await_count
        await_count += 1

    await run_for(
        coroutine=call_on_schedule(
            callback=cb, schedule=schedule.every(0.1).seconds, also_run_immediately=False
        ),
        seconds=0.25,
    )
    assert await_count == 2


@pytest.mark.asyncio
async def test_call_on_schedule_with_long_execution_time(run_for):
    """Execution time should get taken into account"""
    import schedule

    await_count = 0

    async def cb():
        nonlocal await_count
        await_count += 1
        await asyncio.sleep(0.09)

    await run_for(
        coroutine=call_on_schedule(
            callback=cb, schedule=schedule.every(0.1).seconds, also_run_immediately=False
        ),
        seconds=0.25,
    )
    assert await_count == 2


@pytest.mark.asyncio
async def test_run_user_provided_callable_regular_function():
    called = False

    def call_me(a, b):
        nonlocal called
        called = True

    await run_user_provided_callable(call_me, args=[1], kwargs={"b": 2})
    assert called


@pytest.mark.asyncio
async def test_run_user_provided_callable_async_function():
    called = False

    async def call_me(a, b):
        nonlocal called
        called = True

    await run_user_provided_callable(call_me, args=[1], kwargs={"b": 2})
    assert called
