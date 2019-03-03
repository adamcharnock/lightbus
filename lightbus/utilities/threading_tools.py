import asyncio
import logging
import threading

import janus

from lightbus.exceptions import CannotRunInChildThread

logger = logging.getLogger(__name__)


def run_in_main_thread():
    """Decorator to ensure any method invocations are passed to the main thread"""
    destination_thread = threading.current_thread()

    def decorator(fn):
        return_awaitable = asyncio.iscoroutinefunction(fn)

        def wrapper(*args, **kwargs):
            if destination_thread == threading.current_thread():
                return fn(*args, **kwargs)

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

            if isinstance(result, asyncio.Future):
                # Returning futures here really deserves a nice, verbose, explanatory, warning message.
                result = WarningProxy(
                    proxied=result,
                    message=(
                        f"You are trying to access a Future (or Task) which cannot be safely used in this thread.\n\n"
                        f"This Future was returned by {fn.__module__}.{fn.__name__}() and was created within "
                        f"thead {destination_thread} and returned back to thread {threading.current_thread()}.\n\n"
                        f"This functionality was provided by the @run_in_main_thread decorator, which is also the "
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


def assert_in_main_thread():
    def decorator(fn):
        def wrapper(*args, **kwargs):
            # TODO: Improved error message, include function name
            if threading.main_thread() != threading.current_thread():
                raise CannotRunInChildThread(
                    "This functionality cannot be used from within a child thread. "
                    "This functionality must be called within the main thread."
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
