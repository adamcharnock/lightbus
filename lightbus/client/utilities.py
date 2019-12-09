import asyncio
import traceback

from lightbus.exceptions import InvalidName


def validate_event_or_rpc_name(api_name: str, type_: str, name: str):
    """Validate that the given RPC/event name is ok to use"""
    if not name:
        raise InvalidName(f"Empty {type_} name specified when calling API {api_name}")

    if name.startswith("_"):
        raise InvalidName(
            f"You can not use '{api_name}.{name}' as an {type_} because it starts with an underscore. "
            f"API attributes starting with underscores are not available on the bus."
        )


def queue_exception_checker(queue: asyncio.Queue):
    # TODO: wrap this directly around the coroutines instead. Should
    #       allow for saner stack traces
    def queue_exception_checker_(future: asyncio.Future):
        try:
            exception = future.exception()
        except asyncio.CancelledError as e:
            exception = e

        if isinstance(exception, asyncio.CancelledError):
            exception = None

        if exception:
            # TODO: This trace printing needs improving to make it look more normal
            # Gathered tasks won't provide an exception
            if isinstance(future, asyncio.Task) and future.get_stack():
                stack = traceback.format_stack(future.get_stack()[0])
                error = "".join(stack)
            else:
                error = ""

            error += f"\n{exception.__class__.__name__}: {exception}"

            queue.put_nowait(error.strip())

    return queue_exception_checker_
