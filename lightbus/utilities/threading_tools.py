import asyncio
import logging
import threading

import janus

from lightbus.exceptions import CannotRunInChildThread

logger = logging.getLogger(__name__)


def run_in_bus_thread():
    """Decorator to ensure any method invocations are passed to the main thread"""

    def decorator(fn):
        return_awaitable = asyncio.iscoroutinefunction(fn)

        def wrapper(*args, **kwargs):
            # Assume the first arg is the 'self', i.e. the bus client
            bus_client = args[0]

            bus_thread = bus_client._bus_thread

            if threading.current_thread() == bus_thread:
                return fn(*args, **kwargs)

            # We'll provide a queue as the return path for results
            result_queue = janus.Queue()

            # Enqueue the function, it's arguments, and our return path queue
            logger.debug(f"Adding callable {fn.__module__}.{fn.__name__} to queue")
            bus_client._call_queue.sync_q.put((fn, args, kwargs, result_queue))

            # Wait for a return value on the result queue
            logger.debug("Awaiting execution completion")
            result = result_queue.sync_q.get()

            # Cleanup
            bus_client._call_queue.sync_q.join()  # Needed?
            result_queue.close()

            if isinstance(result, asyncio.Future):
                # Returning futures here really deserves a nice, verbose, explanatory, warning message.
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
