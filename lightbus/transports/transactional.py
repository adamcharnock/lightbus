import json
import logging
from asyncio import AbstractEventLoop
from functools import partial
from typing import List, Tuple, Generator, Type, TypeVar, Optional, AsyncIterable, AsyncGenerator

from lightbus.exceptions import (
    LightbusException,
    UnsupportedOptionValue,
    ApisMustUseSameTransport,
    NoApisSpecified,
    DuplicateMessage,
    ApisMustUseTransactionalTransport,
)
from lightbus.schema.encoder import json_encode
from lightbus.serializers import BlobMessageSerializer, BlobMessageDeserializer
from lightbus.transports.base import get_transport, EventTransport, EventMessage
from lightbus.utilities.importing import import_from_string

try:
    import django
except ImportError:
    django = None


try:
    import psycopg2
    import psycopg2.extensions
except ImportError:
    psycopg2 = None


try:
    import aiopg
except ImportError:
    aiopg = None


if False:
    from lightbus.config import Config
    from lightbus.bus import BusNode

logger = logging.getLogger(__name__)


class TransactionalEventTransport(EventTransport):
    """ Combines database interactions and message sending into an atomic unit

    This is done by first writing the messages to the database within the
    same transaction as any other database interactions. Once the transaction is
    complete the messages are placed onto the bus.

    Additionally, incoming messages are de-duplicated.
    """

    def __init__(
        self,
        child_transport: EventTransport,
        database_class: Type["DatabaseConnection"] = "DbApiConnection",
    ):
        self.child_transport = child_transport
        self.database_class = database_class
        self.connection = None
        self.cursor = None
        self.database: DatabaseConnection = None

    @classmethod
    def from_config(
        cls,
        config: "Config",
        # TODO: Figure out how to represent child_transport in the config schema
        #       without circular import problems. May require extracting
        #       the transport selector structures into json schema 'definitions'
        # child_transport: 'EventTransportSelector',
        child_transport: dict,
        database_class: str = "lightbus.transports.transactional.DbApiConnection",
    ):
        transport_name = list(child_transport.keys())[0]
        transport_class = get_transport(type_="event", name=transport_name)
        return cls(
            child_transport=transport_class.from_config(config=config, **child_transport),
            database_class=import_from_string(database_class),
        )

    def set_connection(self, connection, cursor, start_transaction: bool):
        assert (
            not self.connection
        ), "Connection already set. Perhaps you need an lightbus_atomic() context?"
        self.connection = connection
        self.cursor = cursor
        self.database = self.database_class(connection, cursor)
        if start_transaction:
            self.database.start_transaction()

    async def commit_and_finish(self):
        """Commit the current transactions, send committed messages, remove connection"""
        await self.database.commit_transaction()
        await self.publish_pending()  # TODO: Specific ID?
        self._clear_connection()

    async def rollback_and_finish(self):
        """Rollback the current transaction, remove connection"""
        await self.database.rollback_transaction()
        self._clear_connection()

    def _clear_connection(self):
        assert self.connection, "Connection not set. Perhaps you need an lightbus_atomic() context?"
        self.connection = None
        self.cursor = None
        self.database = None

    async def send_event(self, event_message: EventMessage, options: dict):
        await self.database.send_event(event_message, options)

    async def fetch(
        self,
        listen_for: List[Tuple[str, str]],
        context: dict,
        loop: AbstractEventLoop,
        consumer_group: str = None,
        **kwargs,
    ) -> Generator[EventMessage, None, None]:
        assert self.database, "Cannot use this transport outside a lightbus_atomic() context"

        # Needs to:
        # 1. Check if message already processed in messages table
        # 2. Insert message into messages table
        # 3. Yield
        # 4. Commit, catching any exceptions relating to 3.

        consumer = self.child_transport.consume(
            listen_for=listen_for,
            context=context,
            loop=loop,
            consumer_group=consumer_group,
            **kwargs,
        )
        for message in consumer:
            try:
                # Catch duplicate events up-front where possible, rather than
                # perform the processing and get an integrity error later
                if await self.database.is_event_duplicate(message):
                    logger.info(
                        f"Duplicate event {message.canonical_name} detected, ignoring. "
                        f"Event ID: {message.id}"
                    )
                    continue

                self.database.store_processed_event(message)
                yield message
            except Exception:
                logger.warning(
                    f"Error encountered while processing event {message.canonical_name} "
                    f"with ID {message.id}. Rolling back transaction."
                )
                await self.database.rollback_transaction()
                raise
            else:
                try:
                    await self.database.commit_transaction()
                except DuplicateMessage:
                    logger.info(
                        f"Duplicate event {message.canonical_name} discovered upon commit, "
                        f"will rollback transaction. "
                        f"Duplicates were probably being processed simultaneously, otherwise "
                        f"this would have been caught earlier. Event ID: {message.id}"
                    )
                    await self.database.rollback_transaction()

    async def publish_pending(self, message_id=None):
        async for message, options in await self.database.consume_pending_events(message_id):
            await self.child_transport.send_event(message, options)
            await self.database.remove_pending_event(message.id)


class LightbusAtomic(object):

    def __init__(
        self, bus: "BusNode", connection, apis: List[str], start_transaction: Optional[bool] = None
    ):
        # Autodetect transaction starting if not specified
        if start_transaction is None:
            self.start_transaction = self._autodetect_start_transaction(connection)
        else:
            self.start_transaction = start_transaction
        self.connection = connection

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
            return not connection.autocommit
        else:
            return True

    async def _get_cursor(self):
        return self.connection.cursor()

    async def __aenter__(self):
        self.cursor = await self._get_cursor()
        self.transport.set_connection(
            self.connection, self.cursor, start_transaction=self.start_transaction
        )

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            await self.transport.rollback_and_finish()
        else:
            await self.transport.commit_and_finish()


lightbus_atomic = LightbusAtomic

T = TypeVar("T")


def options_deserializer(serilaized):
    if isinstance(serilaized, dict):
        return serilaized
    else:
        return json.loads(serilaized)


class DatabaseConnection(object):
    message_serializer = BlobMessageSerializer()
    message_deserializer = BlobMessageDeserializer(EventMessage)
    options_serializer = partial(json_encode, indent=None, sort_keys=False)
    options_deserializer = staticmethod(options_deserializer)

    def __init__(self, connection, cursor):
        self.connection = connection
        self.cursor = cursor

    async def start_transaction(self):
        raise NotImplementedError()

    async def commit_transaction(self):
        # Should raise DuplicateMessage() if unique key on de-duping table is violated
        raise NotImplementedError()

    async def rollback_transaction(self):
        raise NotImplementedError()

    async def is_event_duplicate(self, message: EventMessage) -> bool:
        raise NotImplementedError()

    async def store_processed_event(self, message: EventMessage):
        # Store message in de-duping table
        raise NotImplementedError()

    async def send_event(self, message: EventMessage, options: dict):
        # Store message in messages-to-be-published table
        raise NotImplementedError()

    async def consume_pending_events(
        self, message_id: Optional[str] = None
    ) -> AsyncIterable[Tuple[EventMessage, dict]]:
        # Should lock row in db
        raise NotImplementedError()

    async def remove_pending_event(self, message_id):
        raise NotImplementedError()


class DbApiConnection(DatabaseConnection):

    async def migrate(self):
        # TODO: smarter version handling
        # TODO: cleanup old entries after x time
        await self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS lightbus_processed_events (
                message_id VARCHAR(100) PRIMARY KEY,
                processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """
        )

        await self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS lightbus_event_outbox (
                message_id VARCHAR(100) PRIMARY KEY,
                message JSON,
                options JSON,
                sent_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """
        )

    async def start_transaction(self):
        await self.cursor.execute("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED")

    async def commit_transaction(self):
        # TODO: Should raise DuplicateMessage() if unique key on de-duping table is violated
        # We do this manually because self.connection.commit() will not work as per:
        #   http://initd.org/psycopg/docs/advanced.html#asynchronous-support
        await self.cursor.execute("COMMIT")

    async def rollback_transaction(self):
        # We do this manually because self.connection.rollback() will not work as per:
        #   http://initd.org/psycopg/docs/advanced.html#asynchronous-support
        await self.cursor.execute("ROLLBACK")

    async def is_event_duplicate(self, message: EventMessage) -> bool:
        sql = "SELECT EXISTS(SELECT 1 FROM lightbus_processed_events WHERE message_id = %s)"
        await self.cursor.execute(sql, [message.id])
        return (await self.cursor.fetchall())[0][0]

    async def store_processed_event(self, message: EventMessage):
        # Store message in de-duping table
        sql = "INSERT INTO lightbus_processed_events (message_id) VALUES (%s)"
        await self.cursor.execute(sql, [message.id])

    async def send_event(self, message: EventMessage, options: dict):
        # Store message in messages-to-be-published table
        try:
            encoded_options = self.options_serializer(options)
        except TypeError as e:
            raise UnsupportedOptionValue(
                f"Failed to encode the provided bus options. These options are provided by the sending code "
                f"when sending an event on the bus. By default this encoding uses JSON, "
                f"although this can be customised. The error was: {e}"
            )
        sql = "INSERT INTO lightbus_event_outbox (message_id, message, options) VALUES (%s, %s, %s)"
        await self.cursor.execute(
            sql, [message.id, self.message_serializer(message), encoded_options]
        )

    async def consume_pending_events(
        self, message_id: Optional[str] = None
    ) -> AsyncGenerator[Tuple[EventMessage, dict], None]:
        # Keep fetching results while there are any to be fetched
        while True:
            # Filter be message ID if specified
            if message_id:
                where = "WHERE message_id = %s"
                args = [message_id]
            else:
                where = ""
                args = []

            # Select a single event from the outbox, ignoring any locked events, and
            # locking the event we select.
            sql = "SELECT message, options FROM lightbus_event_outbox {} LIMIT 1 FOR UPDATE SKIP LOCKED".format(
                where
            )
            await self.cursor.execute(sql, args)
            result = await self.cursor.fetchall()

            if not result:
                return
            else:
                message, options = result[0]
                yield (self.message_deserializer(message), self.options_deserializer(options))

    async def remove_pending_event(self, message_id):
        sql = "DELETE FROM lightbus_event_outbox WHERE message_id = %s"
        await self.cursor.execute(sql, [message_id])


class DjangoConnection(DbApiConnection):

    @classmethod
    async def create(cls: Type[T]) -> T:
        if not django:
            raise DjangoNotInstalled(
                "You are trying to use the Django database for you transactional event transport, "
                "but django could not be imported. Is Django installed? Or perhaps you don't really "
                "want to be using the Django database?"
            )

        # Get the underlying django psycopg2 connection
        return DjangoConnection(connection)

    # ... all the other methods. Need to consult django docs...


class DjangoNotInstalled(LightbusException):
    pass
