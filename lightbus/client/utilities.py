import asyncio
import logging
import sys
from enum import Enum
from functools import wraps
from traceback import extract_stack, StackSummary, format_exception, format_list

from types import TracebackType
from typing import Coroutine, NamedTuple, Type, TYPE_CHECKING, Optional

from lightbus.exceptions import InvalidName
from lightbus.utilities.internal_queue import InternalQueue

logger = logging.getLogger(__name__)


class OnError(Enum):
    SHUTDOWN = "shutdown"
    ACKNOWLEDGE_AND_LOG = "log"


ErrorQueueType = InternalQueue["Error"]


class Error(NamedTuple):
    """An error within a coroutine.

    Specifically, this is typically used to represent errors which
    occurred in child tasks.

    This stores both the data from sys.exc_info(), as well as an additional
    `invoking_stack` value. This `invoking_stack` stores the stack at the point
    where the coroutine was created, whereas the error's traceback is typically local
    to a child asyncio task.

    As a result, we can display a more useful error message. This error message
    can show both the exception and traceback for the error, and also the tracback
    for the location where the coroutine was created.
    """

    type: Type[BaseException]
    value: BaseException
    traceback: Optional[TracebackType]
    invoking_stack: Optional[StackSummary]
    help: str = ""
    exit_code: int = 1

    @property
    def invoking_traceback_str(self) -> str:
        if self.invoking_stack:
            return "".join(format_list(self.invoking_stack)).strip()
        else:
            return ""

    @property
    def exception_str(self) -> str:
        if self.type and self.value and self.traceback:
            return "".join(format_exception(self.type, self.value, self.traceback)).strip()
        else:
            return ""

    def __str__(self):
        return (
            f"A coroutine raised an error, it was invoked at:\n\n"
            f"{self.invoking_traceback_str or 'Not available'}\n\n"
            f"The exception was:\n\n"
            f"{self.exception_str or 'Not available'}"
        )

    def should_show_error(self):
        """Should this error be logged out?"""
        return self.exit_code != 0


def validate_event_or_rpc_name(api_name: str, type_: str, name: str):
    """Validate that the given RPC/event name is ok to use"""
    if not name:
        raise InvalidName(f"Empty {type_} name specified when calling API {api_name}")

    if name.startswith("_"):
        raise InvalidName(
            f"You can not use '{api_name}.{name}' as an {type_} because it starts with an underscore. "
            f"API attributes starting with underscores are not available on the bus."
        )


def queue_exception_checker(coroutine: Coroutine, error_queue: InternalQueue, help: str = ""):
    """Await a coroutine and place any errors into the provided queue

    Items added to the queue will be of the `Error` type. This `Error` type
    allows for more informative error messages.

    See Also: Error
    """
    invoking_stack = extract_stack()[:-1]

    @wraps(coroutine)
    async def coroutine_():
        try:
            await coroutine
        except asyncio.CancelledError:
            # Was cancelled by the caller, so just re-raise the cancelled error and
            # let the coroutine finish as requested
            raise
        except Exception as e:
            # This is an exception which is going to need to be dealt with

            # We set a flag on each exception we add to the queue to indicate
            # that it has been enqueued. This prevents an exception being added to the
            # queue multiple times as it propagates up potentially multiple levels
            # of asyncio tasks (which of which would presumably be wrapped up in queue_exception_checker())
            if not getattr(e, "enqueued", False):
                # Flag not set, so add this error to the queue, and set the flag on it
                e.enqueued = True
                error = Error(*sys.exc_info(), invoking_stack, help)
                await error_queue.put(error)

            raise

    return coroutine_()
