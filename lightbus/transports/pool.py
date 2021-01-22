import threading
from inspect import iscoroutinefunction, isasyncgenfunction
from typing import NamedTuple, List, TypeVar, Type, Generic, TYPE_CHECKING

from lightbus.exceptions import (
    TransportPoolIsClosed,
    CannotShrinkEmptyPool,
    CannotProxySynchronousMethod,
    CannotProxyPrivateMethod,
    CannotProxyProperty,
)

if TYPE_CHECKING:
    # pylint: disable=unused-import,cyclic-import
    from lightbus.config import Config
    from lightbus.transports.base import Transport

    VT = TypeVar("VT", bound=Transport)
else:
    VT = TypeVar("VT")


class TransportPool(Generic[VT]):
    """Pool for managing access to transports

    This pool with function as a transparent proxy to the underlying transports.
    In most cases you shouldn't need to access the underlying transports. If you
    do you can use the context manage as follows:

        async with transport_pool as transport:
            transport.send_event(...)

    Note that this pool will only perform pooling within the thread in which the
    pool was created. If another thread uses the pool then the pool will be bypassed.
    In this case, a new transport will always be created on checkout, and this
    transport will then be immediately closed when checked back in.

    This is because the pool will normally be closed sometime after the thread has
    completed, at which point each transport in the pool will be closed. However, closing
    the transport requires access to the event loop for the specific transport, but that
    loop would have been closed when the thread shutdown. It therefore becomes impossible to
    the transport cleanly. Therefore, in the case of threads, we create new transports on
    checkout, and close and discard the transport on checkin.

    This will have some performance impact for non-async user-provided-callables which need to
    access the bus. These callables area run in a thread, and so will need fresh connections.
    """

    def __init__(self, transport_class: Type[VT], transport_config: NamedTuple, config: "Config"):
        self.transport_class = transport_class
        self.transport_config = transport_config
        self.config = config
        self.closed = False

        self.lock = threading.RLock()
        self.pool: List[VT] = []
        self.checked_out = set()
        self.context_stack: List[VT] = []
        self.home_thread = threading.current_thread()

    def __repr__(self):
        return f"<Pool of {self.transport_class.__name__} at 0x{id(self):02x} to {self}>"

    def __hash__(self):
        return hash((self.transport_class, self.transport_config))

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __str__(self):
        # Here we create an un-opened transport and stringify it.
        # This means we can display nice redis URLs when displaying the pool
        # for debugging output.
        transport = self._instantiate_transport()
        return str(transport)

    async def grow(self):
        with self.lock:
            new_transport = await self._create_transport()
            self.pool.append(new_transport)

    async def shrink(self):
        with self.lock:
            try:
                old_transport = self.pool.pop(0)
            except IndexError:
                raise CannotShrinkEmptyPool(
                    "Transport pool is already empty, cannot shrink it further"
                )
            await self._close_transport(old_transport)

    async def checkout(self) -> VT:
        if self.closed:
            raise TransportPoolIsClosed("Cannot get a connection, transport pool is closed")

        if threading.current_thread() != self.home_thread:
            return await self._create_transport()
        else:
            with self.lock:
                if not self.pool:
                    await self.grow()

                transport = self.pool.pop(0)
                self.checked_out.add(transport)
            return transport

    async def checkin(self, transport: VT):
        if threading.current_thread() != self.home_thread:
            return await self._close_transport(transport)
        else:
            with self.lock:
                self.checked_out.discard(transport)
                self.pool.append(transport)
                if self.closed:
                    await self._close_all()

    @property
    def free(self) -> int:
        return len(self.pool)

    @property
    def in_use(self) -> int:
        return len(self.checked_out)

    @property
    def total(self) -> int:
        return self.free + self.in_use

    async def __aenter__(self) -> VT:
        transport = await self.checkout()
        self.context_stack.append(transport)
        return transport

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        transport = self.context_stack.pop()
        await self.checkin(transport)

    async def close(self):
        with self.lock:
            self.closed = True
            await self._close_all()

    async def _close_all(self):
        with self.lock:
            while self.pool:
                await self._close_transport(self.pool.pop())

    def _instantiate_transport(self) -> VT:
        """Instantiate a transport without opening it"""
        return self.transport_class.from_config(
            config=self.config, **self.transport_config._asdict()
        )

    async def _create_transport(self) -> VT:
        """Return an opened transport"""
        new_transport = self._instantiate_transport()
        await new_transport.open()
        return new_transport

    async def _close_transport(self, transport: VT):
        """Close a specific transport"""
        await transport.close()

    def __getattr__(self, item):
        async def fn_pool_wrapper(*args, **kwargs):
            async with self as transport:
                return await getattr(transport, item)(*args, **kwargs)

        async def gen_pool_wrapper(*args, **kwargs):
            async with self as transport:
                async for value in getattr(transport, item)(*args, **kwargs):
                    yield value

        attr = getattr(self.transport_class, item, None)

        if not attr:
            raise AttributeError(
                f"Neither the transport pool {repr(self)} nor the transport with class "
                f"{repr(self.transport_class)} has an attribute named {item}"
            )
        elif item[0] == "_":
            raise CannotProxyPrivateMethod(
                f"Cannot proxy private method calls to transport. Use the pool's async context or "
                f"checkout() method if you really need to access private methods. (Private methods "
                f"are ones whose name starts with an underscore)"
            )
        elif not callable(attr):
            raise CannotProxyProperty(
                f"Cannot proxy property access on transports. Use the pool's async context or "
                f"checkout() method to get access to a transport directly."
            )
        else:
            if iscoroutinefunction(attr):
                return fn_pool_wrapper
            elif isasyncgenfunction(attr):
                return gen_pool_wrapper
            else:
                raise CannotProxySynchronousMethod(
                    f"{self.transport_class.__name__}.{item}() is synchronous "
                    "and must be accessed directly and not via the pool"
                )
