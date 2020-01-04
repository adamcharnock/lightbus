import asyncio
import threading
from functools import partial
from typing import TypeVar, Generic

T = TypeVar("T")


class InternalQueue(asyncio.Queue, Generic[T]):
    """Threadsafe version of asyncio.Queue"""

    def _init(self, maxsize):
        super()._init(maxsize)
        self._mutex = threading.RLock()
        # We will get the current loop dynamically
        # using asyncio.get_event_loop(), so make sure
        # we splat the parent classes loop to avoid using it
        self._loop = None

    def qsize(self):
        """Number of items in the queue."""
        with self._mutex:
            return len(self._queue)

    def empty(self):
        """Return True if the queue is empty, False otherwise."""
        with self._mutex:
            return not self._queue

    def full(self):
        """Return True if there are maxsize items in the queue.

        Note: if the Queue was initialized with maxsize=0 (the default),
        then full() is never True.
        """
        with self._mutex:
            if self._maxsize <= 0:
                return False
            else:
                return self.qsize() >= self._maxsize

    async def put(self, item: T):
        """Put an item into the queue.

        Put an item into the queue. If the queue is full, wait until a free
        slot is available before adding item.
        """

        with self._mutex:
            try:
                return self.put_nowait(item)
            except asyncio.QueueFull:
                # Queue is full, so start waiting until it has some
                # free space in the following while loop
                pass

            putter = asyncio.get_event_loop().create_future()
            self._putters.append(putter)

        while True:
            try:
                await putter
            except:
                with self._mutex:
                    putter.cancel()  # Just in case putter is not done yet.
                    try:
                        # Clean self._putters from canceled putters.
                        self._putters.remove(putter)
                    except ValueError:
                        # The putter could be removed from self._putters by a
                        # previous get_nowait call.
                        pass
                    if not self.full() and not putter.cancelled():
                        # We were woken up by get_nowait(), but can't take
                        # the call.  Wake up the next in line.
                        self._wakeup_next(self._putters)
                raise

            with self._mutex:
                try:
                    return self.put_nowait(item)
                except asyncio.QueueFull:
                    putter = asyncio.get_event_loop().create_future()
                    self._putters.append(putter)

    def put_nowait(self, item: T):
        """Put an item into the queue without blocking.

        If no free slot is immediately available, raise QueueFull.
        """
        with self._mutex:
            if self.full():
                raise asyncio.QueueFull
            self._put(item)
            self._unfinished_tasks += 1
            self._finished.clear()
            self._wakeup_next(self._getters)

    async def get(self) -> T:
        """Remove and return an item from the queue.

        If queue is empty, wait until an item is available.
        """
        with self._mutex:
            try:
                return self.get_nowait()
            except asyncio.QueueEmpty:
                pass

            getter = asyncio.get_event_loop().create_future()
            self._getters.append(getter)

        while True:
            try:
                await getter
            except:
                with self._mutex:
                    getter.cancel()  # Just in case getter is not done yet.
                    try:
                        # Clean self._getters from canceled getters.
                        self._getters.remove(getter)
                    except ValueError:
                        # The getter could be removed from self._getters by a
                        # previous put_nowait call.
                        pass
                    if not self.empty() and not getter.cancelled():
                        # We were woken up by put_nowait(), but can't take
                        # the call.  Wake up the next in line.
                        self._wakeup_next(self._getters)
                raise

            with self._mutex:
                try:
                    return self.get_nowait()
                except asyncio.QueueEmpty:
                    getter = asyncio.get_event_loop().create_future()
                    self._getters.append(getter)

    def get_nowait(self) -> T:
        """Remove and return an item from the queue.

        Return an item if one is immediately available, else raise QueueEmpty.
        """
        with self._mutex:
            if self.empty():
                raise asyncio.QueueEmpty
            item = self._get()
            self._wakeup_next(self._putters)
            return item

    def _wakeup_next(self, waiters):
        # Wake up the next waiter (if any) that isn't cancelled.
        while waiters:
            waiter = waiters.popleft()
            if not waiter.done():
                # We must set the result from within the waiter's
                # original event loop, so use call_soon_threadsafe()
                waiter.get_loop().call_soon_threadsafe(partial(waiter.set_result, None))
                break

    def task_done(self):
        """Indicate that a formerly enqueued task is complete.

        Used by queue consumers. For each get() used to fetch a task,
        a subsequent call to task_done() tells the queue that the processing
        on the task is complete.

        If a join() is currently blocking, it will resume when all items have
        been processed (meaning that a task_done() call was received for every
        item that had been put() into the queue).

        Raises ValueError if called more times than there were items placed in
        the queue.
        """
        with self._mutex:
            if self._unfinished_tasks <= 0:
                raise ValueError("task_done() called too many times")
            self._unfinished_tasks -= 1
            if self._unfinished_tasks == 0:
                self._finished.set()

    async def join(self):
        """Block until all items in the queue have been gotten and processed.

        The count of unfinished tasks goes up whenever an item is added to the
        queue. The count goes down whenever a consumer calls task_done() to
        indicate that the item was retrieved and all work on it is complete.
        When the count of unfinished tasks drops to zero, join() unblocks.
        """
        with self._mutex:
            if self._unfinished_tasks > 0:
                fut = self._finished.wait()
            else:
                fut = None

        if fut:
            await fut
