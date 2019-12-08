import threading
from typing import NamedTuple, List, TypeVar, Type, TYPE_CHECKING

from lightbus.config import Config
from lightbus.exceptions import TransportPoolIsClosed

if TYPE_CHECKING:
    from lightbus.transports.base import Transport

    GenericTransport = TypeVar("GenericTransport", bound=Transport)


class TransportPool:
    def __init__(
        self,
        transport_class: Type["GenericTransport"],
        transport_config: NamedTuple,
        config: Config,
    ):
        # TODO: Max pool size
        # TODO: Test
        self.transport_class = transport_class
        self.transport_config = transport_config
        self.config = config
        self.closed = False

        self.lock = threading.RLock()
        self.pool: List["GenericTransport"] = []
        self.context_stack: List["GenericTransport"] = []

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

    async def checkout(self) -> "GenericTransport":
        if self.closed:
            raise TransportPoolIsClosed("Cannot get a connection, transport pool is closed")

        with self.lock:
            if not self.pool:
                await self.grow()

            return self.pool.pop(0)

    def checkin(self, transport):
        with self.lock:
            self.pool.append(transport)
            if self.closed:
                self._close_all()

    async def __aenter__(self):
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
