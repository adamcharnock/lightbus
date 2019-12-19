import threading
from inspect import iscoroutinefunction, isasyncgenfunction
from typing import NamedTuple, List, TypeVar, Type, Generic, TYPE_CHECKING

from lightbus.exceptions import TransportPoolIsClosed

if TYPE_CHECKING:
    # pylint: disable=unused-import,cyclic-import
    from lightbus.config import Config
    from lightbus.transports.base import Transport

    VT = TypeVar("VT", bound=Transport)
else:
    VT = TypeVar("VT")


class TransportPool(Generic[VT]):
    def __init__(self, transport_class: Type[VT], transport_config: NamedTuple, config: "Config"):
        # TODO: Max pool size
        # TODO: Test
        self.transport_class = transport_class
        self.transport_config = transport_config
        self.config = config
        self.closed = False

        self.lock = threading.RLock()
        self.pool: List[VT] = []
        self.context_stack: List[VT] = []

    def __repr__(self):
        return f"<Pool of {self.transport_class.__name__} at 0x{id(self):02x}>"

    def __hash__(self):
        return hash((self.transport_class, self.transport_config))

    def __eq__(self, other):
        return hash(self) == hash(other)

    async def grow(self):
        with self.lock:
            new_transport = self.transport_class.from_config(
                config=self.config, **self.transport_config._asdict()
            )
            await new_transport.open()
            self.pool.append(new_transport)

    async def shrink(self):
        with self.lock:
            old_transport = self.pool.pop(0)
            await old_transport.close()

    async def checkout(self) -> VT:
        if self.closed:
            raise TransportPoolIsClosed("Cannot get a connection, transport pool is closed")

        with self.lock:
            if not self.pool:
                await self.grow()

            return self.pool.pop(0)

    def checkin(self, transport: VT):
        with self.lock:
            self.pool.append(transport)
            if self.closed:
                self._close_all()

    async def __aenter__(self) -> VT:
        transport = await self.checkout()
        self.context_stack.append(transport)
        return transport

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        transport = self.context_stack.pop()
        self.checkin(transport)

    async def open(self):
        # TODO: This is used by the lazy loading, which can probably be ditched
        #       now we have moved to using connection pools
        if not self.pool:
            await self.grow()

    async def close(self):
        with self.lock:
            self.closed = True
            await self._close_all()

    async def _close_all(self):
        with self.lock:
            for transport in self.pool:
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

        if item[0] != "_" and attr and callable(attr):
            if iscoroutinefunction(attr):
                return fn_pool_wrapper
            elif isasyncgenfunction(attr):
                return gen_pool_wrapper
            else:
                # TODO: Proper exception
                raise Exception(
                    f"{self.transport_class.__name__}.{item}() is synchronous "
                    f"and must be accessed directly and not via the pool"
                )
        else:
            # Will likely raise an error
            return getattr(self, item)
