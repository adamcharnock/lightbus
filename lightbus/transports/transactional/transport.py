import logging
from asyncio import AbstractEventLoop
from typing import List, Tuple, Generator, Type

from lightbus.exceptions import DuplicateMessage, LightbusException
from lightbus.transports.base import get_transport, EventTransport, EventMessage
from lightbus.utilities.importing import import_from_string
from .databases import DbApiConnection, DatabaseConnection

if False:
    from lightbus.config import Config


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
        database_class: Type[DatabaseConnection] = DbApiConnection,
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
            child_transport=transport_class.from_config(
                config=config, **child_transport[transport_name]
            ),
            database_class=import_from_string(database_class),
        )

    async def set_connection(self, connection, cursor, start_transaction: bool):
        if self.connection:
            raise ConnectionAlreadySet(
                f"The transactional transport connection has already been set. "
                f"Perhaps you should use a lightbus_atomic() context?"
            )
        self.connection = connection
        self.cursor = cursor
        self.database = self.database_class(connection, cursor)
        if start_transaction:
            await self.database.start_transaction()

    async def commit_and_finish(self):
        """Commit the current transactions, send committed messages, remove connection"""
        if not self.database:
            raise DatabaseNotSet(
                f"The transactional transport database connection has not been setup. "
                f"Perhaps you should use a lightbus_atomic() context?"
            )
        await self.database.commit_transaction()
        await self.publish_pending()  # TODO: Specific ID?
        self._clear_connection()

    async def rollback_and_finish(self):
        """Rollback the current transaction, remove connection"""
        if not self.database:
            raise DatabaseNotSet(
                f"The transactional transport database connection has not been setup. "
                f"Perhaps you should use a lightbus_atomic() context?"
            )
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
        async for message, options in self.database.consume_pending_events(message_id):
            await self.child_transport.send_event(message, options)
            await self.database.remove_pending_event(message.id)


class ConnectionAlreadySet(LightbusException):
    pass


class DatabaseNotSet(LightbusException):
    pass
