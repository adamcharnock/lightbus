import asyncio
from concurrent.futures import ThreadPoolExecutor

import janus

from lightbus.utilities.async_tools import block, cancel, make_exception_checker, get_event_loop

import logging


# Logging setup


class AsyncioLoggingFilter(logging.Filter):
    def filter(self, record):
        task = asyncio.Task.current_task()

        record.task = f'[task {id(task)}]' if task else '[NOLOOP         ]'
        return True


logger = logging.getLogger(__name__)
logger.addFilter(AsyncioLoggingFilter())
logging.getLogger('asyncio').setLevel(logging.WARNING)


logging.basicConfig(level=logging.DEBUG, format="%(msecs)f %(threadName)s %(task)s %(msg)s")

thread_pool_executor = ThreadPoolExecutor(thread_name_prefix="dispatch")


# Utilities


async def execute_in_thread(fn):
    def new_thread_setup():
        # Make sure each thread has an event loop available, even if it
        # isn't actually running (stops janus.Queue barfing)
        get_event_loop()
        return fn()

    # Run the given blocking fn in a thread
    return await asyncio.get_event_loop().run_in_executor(
        executor=thread_pool_executor,
        func=new_thread_setup
    )


def run_in_main_thread():
    """Decorator to ensure any method invocations are passed to the main thread"""

    def decorator(fn):
        def wrapper(*args, **kwargs):
            # Assume the first arg is the 'self', i.e. the bus client
            bus = args[0]

            # We'll provide a queue as the return path for results
            result_queue = janus.Queue()

            # Enqueue the function, it's arguments, and our return path queue
            logger.debug("Adding callable to queue")
            bus._call_queue.sync_q.put((fn, args, kwargs, result_queue))

            # Wait for a return value on the result queue
            logger.debug("Awaiting execution completion")
            result = result_queue.sync_q.get()

            # Cleanup
            bus._call_queue.sync_q.join()  # Needed?
            result_queue.close()

            return result
        return wrapper
    return decorator


# Simplified bus client


class Bus(object):

    def __init__(self):
        self._call_queue = janus.Queue()

    def open(self):
        logger.debug("Bus is opening")

        # Start a background task running to handling incoming calls on the _perform_calls queue
        self._perform_calls_task = asyncio.ensure_future(self._perform_calls())

        # Housekeeping for error handling
        self._perform_calls_task.add_done_callback(make_exception_checker())

    @run_in_main_thread()
    async def call_rpc(self, name, bus_client: "BusClient"):
        # We'd normally do all the Lightbus goodness here
        await asyncio.sleep(0.1)

        # Causes deadlock because the dispatch thread is already in use
        await self.fire_event()

        return "Return value"

    @run_in_main_thread()
    async def fire_event(self, name):
        # We'd normally do all the Lightbus goodness here
        await asyncio.sleep(0.1)

    async def _perform_calls(self):
        """Coroutine to run in background consuming incoming calls from child threads

        Launched in Bus.open()
        """
        while True:
            # Wait for calls
            logger.debug("Awaiting calls on the call queue")
            fn, args, kwargs, result_queue = await self._call_queue.async_q.get()

            # Execute call
            logger.debug("Call received, executing")
            result = await fn(*args, **kwargs)

            # Acknowledge the completion
            logger.debug("Call executed, marking as done")
            self._call_queue.async_q.task_done()

            # Put the result in the result queue
            logger.debug("Returning result")
            await result_queue.async_q.put(result)

    def close(self):
        logger.debug("Bus is closing")
        self._call_queue.sync_q.join()
        block(cancel(self._perform_calls_task))


bus = Bus()


# Descend in into nested sync/async madness


async def one():
    # Runs in event loop
    logger.debug("-> One started")
    await execute_in_thread(two)
    logger.debug("-> One finished")


def two():
    # Blocks!
    logger.debug("--> Two started")
    block(three())
    logger.debug("--> Two finished")


async def three():
    # Now running in event loop again
    logger.debug("---> Three started")
    await four()
    logger.debug("---> Three finished")


async def four():
    # Run some blocking code
    logger.debug("----> Four started")
    await execute_in_thread(five)
    logger.debug("----> Four finished")


def five():
    # Now call an RPC (an async method!)
    logger.debug("-----> Five started")
    rpc_result = bus.call_rpc(name='foo')
    logger.debug(f"-----> Five finished (result: {rpc_result})")


if __name__ == '__main__':
    bus.open()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(one())
    bus.close()
