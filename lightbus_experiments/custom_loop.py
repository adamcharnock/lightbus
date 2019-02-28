import asyncio
import logging


# Logging setup
import threading
import time
from concurrent.futures import ThreadPoolExecutor


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




class CustomTask(asyncio.Task):
    _lock = threading.Lock()

    def _wakeup(self, *args, **kwargs):
        logger.debug("Acquire lock")
        CustomTask._lock.acquire()

        super()._wakeup(*args, **kwargs)

        logger.debug("Releasing lock")
        CustomTask._lock.release()


def task_factory(loop, coro):
    return CustomTask(coro, loop=loop)


async def one():
    await asyncio.sleep(0.01)
    logger.debug("-> One")
    await two()
    await asyncio.sleep(0.01)
    logger.debug("-> Exiting one")


async def two():
    await asyncio.sleep(0.01)

    logger.debug("--> Two")
    time.sleep(0.01)
    logger.debug("--> Two")
    time.sleep(0.01)
    logger.debug("--> Two")


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.set_task_factory(task_factory)
    loop.run_until_complete(one())
