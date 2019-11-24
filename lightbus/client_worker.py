import asyncio
import inspect
import logging
import queue
import threading
import traceback
from functools import partial
from typing import Callable

import janus

from lightbus.exceptions import (
    MustRunInBusThread,
    MustNotRunInBusThread,
    WorkerDeadlock,
    WorkerNotReady,
)
from lightbus.utilities.async_tools import make_exception_checker, block, cancel

logger = logging.getLogger(__name__)


def _get_worker_from_function_args(*args, **kwargs) -> "ClientWorker":
    # Assume the first arg is 'self'
    if isinstance(args[0], ClientWorker):
        # This is the worker itself
        return args[0]
    else:
        # This is the bus client
        return args[0].worker


def run_in_worker_thread(worker=None):
    """Decorator to ensure any method invocations are passed to the worker thread"""

    def decorator(fn):
        return_awaitable = asyncio.iscoroutinefunction(fn)

        def wrapper(*args, **kwargs):
            nonlocal wrapper
            current_frame = inspect.currentframe()

            worker_ = worker or _get_worker_from_function_args(*args, **kwargs)
            worker_thread = worker_._thread
            if threading.current_thread() == worker_thread:
                return fn(*args, **kwargs)

            logger.debug(
                f"Proxying call for {fn.__module__}.{fn.__name__} to worker thread {worker_thread.name}."
            )

            if not worker_._ready.is_set():
                raise WorkerNotReady(
                    f"The worker thread {worker_thread.name} is not ready to accept calls. Either it "
                    f"has not yet started, or it has been shutdown. "
                    f"The called function was {fn.__module__}.{fn.__name__}. "
                    f"Turning on debug logging (using --log-level=debug) will give you a better idea of what is "
                    f"going on."
                )

            if worker_._call_queue.sync_q.unfinished_tasks:
                unfinished_traceback = traceback.format_list(
                    traceback.extract_stack(f=worker_._current_frame, limit=5)[:-1]
                )

                if hasattr(wrapper, "_parent_stack"):
                    new_stack = wrapper._parent_stack
                else:
                    new_stack = traceback.extract_stack(f=current_frame, limit=5)
                new_traceback = traceback.format_list(new_stack)

                raise WorkerDeadlock(
                    f"Deadlock detected: Calling {fn.__module__}.{fn.__name__} requires use of the Lightbus "
                    f"client worker thread. However, that thread is already busy handling the call which got "
                    f"us here in the first place.\n"
                    f"\n"
                    f"The thread was busy handling a call at:\n"
                    f"{''.join(unfinished_traceback)}\n"
                    f"The blocked incoming call originated at:\n\n"
                    f"{''.join(new_traceback)}\n"
                )

            # We'll provide a queue as the return path for results
            result_queue = queue.Queue()

            # Enqueue the function, it's arguments, and our return path queue
            worker_._call_queue.sync_q.put((fn, args, kwargs, current_frame, result_queue))

            # Wait for a return value on the result queue
            logger.debug("Awaiting execution completion")
            result = result_queue.get()
            result_queue.task_done()

            logger.debug("Execution completed")

            # Cleanup
            worker_._call_queue.sync_q.join()  # Needed?

            if isinstance(result, asyncio.Future):
                # Wrap any returned futures in a WarningProxy, which will show a nice
                # verbose warning should the user try to use the future
                result = WarningProxy(
                    proxied=result,
                    message=(
                        f"You are trying to access a Future (or Task) which cannot be safely used in this thread.\n\n"
                        f"This Future was returned by {fn.__module__}.{fn.__name__}() and was created within "
                        f"thead {worker_thread.name} and returned back to thread {threading.current_thread()}.\n\n"
                        f"This functionality was provided by the @run_in_worker_thread decorator, which is also the "
                        f"source of this message.\n\n"
                        f"Reason: Futures are tied to a specific event loop, and event loops are thread-specific. "
                        f"Passing futures between threads is therefore a really bad idea, as the future will be "
                        f"useless outside of the event loop in which it was created."
                    ),
                )
            elif isinstance(result, Exception):
                raise result

            if return_awaitable:
                future = asyncio.Future()
                future.set_result(result)
                return future
            else:
                return result

        # Indicates to run_user_provided_callable() that a stack trace can be attached
        # to this callable. This is useful for error reporting
        wrapper._parent_stack = None

        return wrapper

    return decorator


def assert_in_worker_thread():
    """Decorator. Raise an error if the decorated function was *not* called from the worker thread"""

    def decorator(fn):
        def wrapper(*args, **kwargs):
            worker = _get_worker_from_function_args(*args, **kwargs)

            if threading.current_thread() != worker._thread:
                raise MustRunInBusThread(
                    f"This function ({fn.__module__}.{fn.__name__}) may only be called from "
                    f"within the bus client's worker thread ({worker._thread.name}). The function "
                    f"was called within {threading.current_thread().name}."
                )
            return fn(*args, **kwargs)

        return wrapper

    return decorator


def assert_not_in_worker_thread():
    """Decorator. Raise an error if the decorated function *was* called from the worker thread"""

    def decorator(fn):
        def wrapper(*args, **kwargs):
            worker = _get_worker_from_function_args(*args, **kwargs)

            if threading.current_thread() == worker._thread:
                raise MustNotRunInBusThread(
                    f"This function ({fn.__module__}.{fn.__name__}) may NOT be called from "
                    f"within the bus client's worker thread ({worker._thread.name})."
                )
            return fn(*args, **kwargs)

        return wrapper

    return decorator


class WarningProxy:
    """Utility to help displaying of a nice verbose error message

    See above use of WarningProxy.
    """

    def __init__(self, proxied, message):
        self.message = message
        self.proxied = proxied

    def __repr__(self):
        return f"<WarningProxy: {super().__repr__()}>"

    def __getattr__(self, item):
        logger.warning(self.message)
        return getattr(self.proxied, item)

    def __await__(self):
        return self.__getattr__("__await__")()


class WorkerProxy:
    """Proxy all attribute access for the given object to the given ClientWorker"""

    def __init__(self, proxied, worker):
        self._proxied = proxied
        self._worker = worker

    def __repr__(self):
        return f"<WorkerProxy: {super().__repr__()}>"

    def __getattr__(self, item):
        value = getattr(self._proxied, item)
        if threading.current_thread() == self._worker._thread:
            return value

        if callable(value):
            return run_in_worker_thread(worker=self._worker)(value)
        else:
            return value

    def __contains__(self, item):
        return self.__getattr__("__contains__")(item)


class ClientWorker:
    """Management of the client worker thread

    Lightbus performs the bulk of its bus interactions in its own worker thread.
    This stems from that fact that we often have to run blocking user-specified
    code in an async fashion (see `run_user_provided_callable()`). In order
    to do this we offload the callable to its own thread. However, that thread then
    often needs to access the bus (for firing events, calling RPCs). This then introduces a
    problem because bus access requires access to global resources (such as Redis connections).
    We therefore have this client thread which all these interactions are handed of too.

    This hand-off is provided by the @run_in_worker_thread(), and is often used in the
    BusClient class.
    """

    _lock = threading.Lock()
    _TOTAL_WORKERS = 0

    def __init__(self):
        self._call_queue = None
        self._worker_shutdown_queue = None
        self._thread = None
        self._ready = threading.Event()
        self._current_frame = None

    def start(self, bus_client, after_shutdown: Callable = None):
        # TODO: Remove need for bus_client as an argument here
        with self._lock:
            ClientWorker._TOTAL_WORKERS += 1

        self._thread = threading.Thread(
            name=f"LightbusThread{ClientWorker._TOTAL_WORKERS}",
            target=partial(self.worker, bus_client=bus_client, after_shutdown=after_shutdown),
            daemon=True,
        )
        logger.debug(f"Starting bus thread {self._thread.name}. Will wait until it is ready")
        self._thread.start()
        self._ready.wait()
        logger.debug(f"Waiting over, bus thread {self._thread.name} is now ready")

    @assert_not_in_worker_thread()
    def shutdown(self):
        logger.debug(f"Sending shutdown message to bus thread {self._thread.name}")
        if not self._thread.is_alive():
            # Already shutdown, move along
            return

        try:
            self._worker_shutdown_queue.sync_q.put(None)
        except RuntimeError:
            pass

    @assert_not_in_worker_thread()
    async def wait_for_shutdown(self):
        logger.debug("Waiting for thread to finish")
        for _ in range(0, 50):
            if self._thread.is_alive():
                await asyncio.sleep(0.1)
            else:
                self._thread.join()

        if self._thread.is_alive():
            logger.error("Worker thread failed to shutdown in a timely fashion")
            # TODO: Kill painfully?
        else:
            logger.debug("Worker thread shutdown cleanly")

    def worker(self, bus_client, after_shutdown: Callable = None):
        """ Starting point for the worker thread

        ---

        A note about error handling in the worker thread:

        There are two scenarios in which the worker thread my encounter an error.

            1. The bus is being used as a client. A bus method is called by the client code,
               and this call raises an exception. This exception is propagated to the client
               code for it to deal with.
            2. The bus is being used as a server and has various coroutines running at any one
               time. In this case, if a coroutine encounters an error then it should cause the
               lightbus server to exit.

        In response to either of these cases the bus needs to shut itself down. Therefore,
        the worker needs to keep on running for a while in order to handle the various shutdown tasks.

        In case 1 above, we assume the developer will take responsibility for closing the bus
        correctly when they are done with it.

        In case 2 above, the worker needs to signal the main lightbus run process to tell it to begin the
        shutdown procedure

        """
        logger.debug(f"Bus thread {self._thread.name} initialising")

        # Start a new event loop for this new thread
        asyncio.set_event_loop(asyncio.new_event_loop())

        self._call_queue = janus.Queue()
        self._worker_shutdown_queue = janus.Queue()

        async def worker_shutdown_monitor():
            await self._worker_shutdown_queue.async_q.get()
            asyncio.get_event_loop().stop()
            self._worker_shutdown_queue.async_q.task_done()

        shutdown_monitor_task = asyncio.ensure_future(worker_shutdown_monitor())
        shutdown_monitor_task.add_done_callback(make_exception_checker(bus_client, die=True))

        perform_calls_task = asyncio.ensure_future(self.perform_calls())
        perform_calls_task.add_done_callback(make_exception_checker(bus_client, die=True))

        self._ready.set()

        asyncio.get_event_loop().run_forever()

        logging.debug(f"Event loop stopped in bus worker thread {self._thread.name}. Closing down.")
        self._ready.clear()

        if after_shutdown:
            after_shutdown()

        logger.debug("Canceling worker tasks")
        block(cancel(perform_calls_task, shutdown_monitor_task))

        logger.debug("Closing the call queue")
        self._call_queue.close()
        block(self._call_queue.wait_closed())

        logger.debug("Closing the worker shutdown queue")
        self._worker_shutdown_queue.close()
        block(self._worker_shutdown_queue.wait_closed())

        logger.debug("Worker shutdown complete")

    async def perform_calls(self):
        """Coroutine to run in background consuming incoming calls from other threads
        """
        # pylint: disable=broad-except
        # TODO: The name 'perform_calls' is too generic, find all uses and rename to something better
        while True:
            # Wait for calls
            logger.debug("Awaiting calls on the call queue")
            fn, args, kwargs, current_frame, result_queue = await self._call_queue.async_q.get()

            self._current_frame = current_frame

            # Execute call
            logger.debug(f"Call to {fn.__name__} received, executing")
            try:
                result = fn(*args, **kwargs)
                if inspect.isawaitable(result):
                    result = await result
            except asyncio.CancelledError as e:
                raise
            except Exception as e:
                result = e
            finally:
                # Acknowledge the completion
                logger.debug("Call executed, marking as done")
                self._call_queue.async_q.task_done()
                self._current_frame = None

            # Put the result in the result queue
            logger.debug(f"Returning result {result}")
            result_queue.put(result)
