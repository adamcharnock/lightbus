import asyncio
import logging

from inspect import isawaitable
from typing import List, Optional

from lightbus.exceptions import (
    ApisMustUseSameTransport,
    NoApisSpecified,
    ApisMustUseTransactionalTransport,
)
from lightbus.transports.transactional.transport import TransactionalEventTransport
from lightbus.utilities.async_tools import block, get_event_loop

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
    from lightbus.path import BusPath


logger = logging.getLogger(__name__)


class LightbusDbContext(object):
    def __init__(
        self,
        bus: "BusPath",
        connection,
        apis: List[str] = ("default",),
        manage_transaction: Optional[bool] = True,
        cursor=None,
    ):
        self.manage_transaction = manage_transaction
        self.connection = connection
        self.custom_cursor = cursor

        # Get the transport, and check it is sane
        transports = list(
            {bus.client.transport_registry.get_event_transport(api_name) for api_name in apis}
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

    async def _get_cursor(self):
        if self.custom_cursor:
            return self.custom_cursor
        else:
            cursor = self.connection.cursor()
            if isawaitable(cursor):
                cursor = await cursor
            return cursor

    def __enter__(self):
        block(self.__aenter__(), timeout=5)

    def __exit__(self, exc_type, exc_val, exc_tb):
        block(self.__aexit__(exc_type, exc_val, exc_tb), timeout=5)

    async def __aenter__(self):
        self.cursor = await self._get_cursor()
        await self.transport.set_connection(self.connection, self.cursor)
        if self.manage_transaction:
            await self.transport.start_transaction()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # This sleep statement is a hack. We need to ensure that we yield control
        # to any transactional transport listeners that want to set themselves up.
        # In particular, they need to grab a reference to the database before we
        # clean it up below
        await asyncio.sleep(0.0001)

        if self.manage_transaction:
            if exc_type:
                logger.debug(f"Event handler raised {exc_type}. Rolling back transaction")
                await self.transport.rollback_and_finish()
            else:
                logger.debug(f"Event handler completed successfully. Committing back transaction")
                await self.transport.commit_and_finish()
        self.cursor = None


lightbus_set_database = LightbusDbContext
