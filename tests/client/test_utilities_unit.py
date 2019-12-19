import asyncio
import traceback

import pytest

from lightbus.client.utilities import queue_exception_checker, Error

pytestmark = pytest.mark.unit


class ExampleException(Exception):
    pass


@pytest.fixture
def erroring_coroutine():
    async def erroring_coroutine_():
        raise ExampleException

    return erroring_coroutine_


@pytest.fixture
def ok_coroutine():
    async def ok_coroutine_():
        pass

    return ok_coroutine_


@pytest.fixture
def error_queue():
    return asyncio.Queue()


@pytest.mark.asyncio
async def test_queue_exception_checker_directly(erroring_coroutine, error_queue: asyncio.Queue):
    coroutine = queue_exception_checker(erroring_coroutine(), error_queue)

    with pytest.raises(ExampleException):
        await coroutine

    assert error_queue.qsize() == 1
    error: Error = error_queue.get_nowait()

    assert error.type == ExampleException
    assert isinstance(error.value, ExampleException)
    assert "test_utilities_unit.py" in str(error)
    assert "ExampleException" in str(error)


def test_queue_exception_checker_in_task(erroring_coroutine, error_queue: asyncio.Queue):
    coroutine = queue_exception_checker(erroring_coroutine(), error_queue)

    with pytest.raises(ExampleException):
        asyncio.run(coroutine)

    assert error_queue.qsize() == 1
    error: Error = error_queue.get_nowait()

    assert error.type == ExampleException
    assert isinstance(error.value, ExampleException)
    assert "test_utilities_unit.py" in str(error)
    assert "ExampleException" in str(error)
