from inspect import isawaitable
from typing import List, Optional

from lightbus.exceptions import (
    ApisMustUseSameTransport,
    NoApisSpecified,
    ApisMustUseTransactionalTransport,
)
from lightbus.transports.transactional.transport import TransactionalEventTransport

try:
    import django
except ImportError:  # pragma: no cover
    django = None


try:
    import psycopg2
    import psycopg2.extensions
except ImportError:  # pragma: no cover
    psycopg2 = None


try:
    import aiopg
except ImportError:  # pragma: no cover
    aiopg = None

if False:
    from lightbus.bus import BusNode


class LightbusAtomic(object):

    def __init__(
        self,
        bus: "BusNode",
        connection,
        apis: List[str] = tuple("default"),
        start_transaction: Optional[bool] = None,
        cursor=None,
    ):
        # Autodetect transaction starting if not specified
        if start_transaction is None:
            self.start_transaction = self._autodetect_start_transaction(connection)
        else:
            self.start_transaction = start_transaction
        self.connection = connection
        self.custom_cursor = cursor

        # Get the transport, and check it is sane
        transports = list(
            {bus.bus_client.transport_registry.get_event_transport(api_name) for api_name in apis}
        )
        if len(transports) > 1:
            # This check is not strictly required, but let's enforce it
            # for the initial implementation.
            raise ApisMustUseSameTransport(
                f"Multiple APIs were specified when starting a lightbus atomic "
                f"context, but all APIs did not share the same transport. APIs to be "
                f"used within the same block must use the same transactional event "
                f"transport. The specified APIs were: {', '.join(apis)}"
            )
        elif not transports:
            raise NoApisSpecified(
                f"No APIs were specified when starting a lightbus atomic context. At least "
                f"one API must be specified, and each API must use the same transactional event "
                f"transport."
            )
        elif not isinstance(transports[0], TransactionalEventTransport):
            raise ApisMustUseTransactionalTransport(
                f"The specified APIs are not configured to use the TransactionalEventTransport "
                f"transport. Instead they use {transports[0].__class__}. The "
                f"specified APIs were: {', '.join(apis)}."
            )

        self.transport: TransactionalEventTransport = transports[0]

    def _autodetect_start_transaction(self, connection) -> Optional[bool]:
        if psycopg2 and isinstance(connection, psycopg2.extensions.connection):
            return not connection.autocommit
        if aiopg and isinstance(connection, aiopg.Connection):
            # aiopg must always have autocommit on in order for the underlying
            # psycopg2 library for function in asynchronous mode
            return False
        else:
            # Issuing a BEGIN statement twice is not an error, so we can be
            # liberal about it
            return True

    async def _get_cursor(self):
        if self.custom_cursor:
            return self.custom_cursor
        else:
            cursor = self.connection.cursor()
            if isawaitable(cursor):
                cursor = await cursor
            return cursor

    async def __aenter__(self):
        self.cursor = await self._get_cursor()
        await self.transport.set_connection(
            self.connection, self.cursor, start_transaction=self.start_transaction
        )

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            await self.transport.rollback_and_finish()
        else:
            await self.transport.commit_and_finish()
        self.cursor = None


lightbus_atomic = LightbusAtomic
