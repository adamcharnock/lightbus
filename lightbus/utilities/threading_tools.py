import asyncio
import logging
import queue
import threading

import janus

from lightbus.exceptions import CannotRunInChildThread

logger = logging.getLogger(__name__)


def run_in_bus_thread(bus_client=None):
    """Decorator to ensure any method invocations are passed to the main thread"""

    def decorator(fn):
        return_awaitable = asyncio.iscoroutinefunction(fn)

        def wrapper(*args, **kwargs):
            # Assume the first arg is the 'self', i.e. the bus client
            bus_client_ = bus_client or args[0]

            bus_thread = bus_client_._bus_thread

            if threading.current_thread() == bus_thread:
                return fn(*args, **kwargs)

            logger.debug(
                f"Proxying call for {fn.__module__}.{fn.__name__} to bus thread {bus_thread.name}."
            )

            if not bus_client_._bus_thread_ready.is_set():
                # TODO: Improve exception
                raise Exception(
                    f"The worker thread {bus_thread.name} is not ready to accept calls. Either it "
                    f"has not yet started, or it has been shutdown. "
                    f"The called function was {fn.__module__}.{fn.__name__}. "
                    f"Turning on debug logging (using --log-level=debug) will give you a better idea of what is "
                    f"going on."
                )

            # We'll provide a queue as the return path for results
            result_queue = queue.Queue()

            # Enqueue the function, it's arguments, and our return path queue
            bus_client_._call_queue.sync_q.put((fn, args, kwargs, result_queue))

            # Wait for a return value on the result queue
            logger.debug("Awaiting execution completion")
            result = result_queue.get()
            result_queue.task_done()

            logger.debug("Execution completed")

            # Cleanup
            bus_client_._call_queue.sync_q.join()  # Needed?

            if isinstance(result, asyncio.Future):
                # Wrap any returned futures in a WarningProxy, which will show a nice
                # verbose warning should the user try to use the future
                result = WarningProxy(
                    proxied=result,
                    message=(
                        f"You are trying to access a Future (or Task) which cannot be safely used in this thread.\n\n"
                        f"This Future was returned by {fn.__module__}.{fn.__name__}() and was created within "
                        f"thead {bus_thread} and returned back to thread {threading.current_thread()}.\n\n"
                        f"This functionality was provided by the @run_in_bus_thread decorator, which is also the "
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

        return wrapper

    return decorator


def assert_in_bus_thread():
    def decorator(fn):
        def wrapper(*args, **kwargs):
            # Assume the first arg is the 'self', i.e. the bus client
            bus = args[0]

            if threading.current_thread() != bus._bus_thread:
                raise CannotRunInChildThread(
                    f"This function ({fn.__module__}.{fn.__name__}) may only be called from "
                    f"within the bus client's home thread ({bus._bus_thread.name}). The function "
                    f"as actually called from {threading.current_thread().name}."
                )
            return fn(*args, **kwargs)

        return wrapper

    return decorator


class WarningProxy(object):
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


class BusThreadProxy(object):
    def __init__(self, proxied, bus_client):
        self._proxied = proxied
        self._bus_client = bus_client

    def __repr__(self):
        return f"<BusThreadProxy: {super().__repr__()}>"

    def __getattr__(self, item):
        value = getattr(self._proxied, item)
        if threading.current_thread() == self._bus_client._bus_thread:
            return value

        if callable(value):
            return run_in_bus_thread(bus_client=self._bus_client)(value)
        else:
            return value

    def __contains__(self, item):
        return self.__getattr__("__contains__")(item)
