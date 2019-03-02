import asyncio
import logging
import threading

import janus

from lightbus.exceptions import InvalidReturnValue

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
                raise InvalidReturnValue(
                    f"Callable {fn.__module__}.{fn.__name__}() decorated by @run_in_main_thread() returned a future. "
                    f"The @run_in_main_thread() does not support returning futures.\n\n"
                    f"Futures are tied to a specific event loop, and event loops are thread-specific. Passing futures "
                    f"between threads is therefore a really bad idea, as the future will be useless outside of the "
                    f"event loop which created it."
                )

            if return_awaitable:
                future = asyncio.Future()
                future.set_result(result)
                return future
            else:
                return result

        return wrapper

    return decorator
