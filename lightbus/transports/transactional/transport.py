import logging
import threading
from asyncio import AbstractEventLoop
from typing import List, Tuple, Generator, Type, AsyncGenerator

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
    # TODO: Note in docs that this transport will not work with the metrics and state APIs

    def __init__(
        self,
        child_transport: EventTransport,
        database_class: Type[DatabaseConnection] = DbApiConnection,
        auto_migrate: bool = True,
    ):
        self.child_transport = child_transport
        self.database_class = database_class
        self.auto_migrate = auto_migrate
        self._migrated = False
        self._local = threading.local()

    @property
    def connection(self):
        return getattr(self._local, "connection", None)

    @connection.setter
    def connection(self, value):
        self._local.connection = value

    @property
    def cursor(self):
        return getattr(self._local, "cursor", None)

    @cursor.setter
    def cursor(self, value):
        self._local.cursor = value

    @property
    def database(self) -> DatabaseConnection:
        return getattr(self._local, "database", None)

    @database.setter
    def database(self, value):
        self._local.database = value

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
        auto_migrate: bool = True,
    ):
        transport_name = list(child_transport.keys())[0]
        transport_class = get_transport(type_="event", name=transport_name)
        return cls(
            child_transport=transport_class.from_config(
                config=config, **child_transport[transport_name]
            ),
            database_class=import_from_string(database_class),
            auto_migrate=auto_migrate,
        )

    async def set_connection(self, connection, cursor):
        if self.connection:
            raise ConnectionAlreadySet(
                f"The transactional transport connection has already been set. "
                f"Perhaps you should use a lightbus_set_database() context?"
            )
        self.connection = connection
        self.cursor = cursor
        self.database = self.database_class(connection, cursor)
        if self.auto_migrate and not self._migrated:
            await self.database.migrate()

    async def start_transaction(self):
        await self.database.start_transaction()

    async def commit_and_finish(self):
        """Commit the current transactions, send committed messages, remove connection"""
        if not self.database:
            raise DatabaseNotSet(
                f"You are using the transactional event transport but the transport's "
                f"database connection has not been setup. "
                f"Perhaps you should use a lightbus_set_database() context?"
            )
        await self.database.commit_transaction()
        await self.database.start_transaction()

        try:
            await self.publish_pending()
        except BaseException:
            await self.database.rollback_transaction()
            raise
        else:
            await self.database.commit_transaction()

        self._clear_connection()

    async def rollback_and_finish(self):
        """Rollback the current transaction, remove connection"""
        if not self.database:
            raise DatabaseNotSet(
                f"You are using the transactional event transport but the transport's "
                f"database connection has not been setup. "
                f"Perhaps you should use a lightbus_set_database() context?"
            )
        await self.database.rollback_transaction()
        self._clear_connection()

    def _clear_connection(self):
        assert (
            self.connection
        ), "Connection not set. Perhaps you need an lightbus_set_database() context?"
        self.connection = None
        self.cursor = None
        self.database = None

    async def send_event(self, event_message: EventMessage, options: dict):
        if not self.database:
            raise DatabaseNotSet(
                f"You are using the transactional event transport for event "
                f"{event_message.canonical_name}, but the transactional transport's "
                f"database connection has not been setup. This is typically done using "
                f"a lightbus_set_database() context."
            )
        await self.database.send_event(event_message, options)

    async def consume(
        self, listen_for: List[Tuple[str, str]], consumer_group: str = None, **kwargs
    ) -> AsyncGenerator[EventMessage, None]:
        self._sanity_check_listen_for(listen_for)

        if not self.database:
            raise DatabaseNotSet(
                f"You are trying to consume events on APIs { {a for a, e in listen_for} }. "
                f"These APIs are configured to use the transaction transport. However, "
                f"no database has been set on the transport. You should do this using the "
                f"lightbus_set_database() async context. "
            )

        # IMPORTANT: Because this method yields control, it is possible that
        #            the reference to self.database will change during execution,
        #            we therefore create a local reference to the database here
        #            as we want to ensure we use the same database throughout all
        #            the code below.
        #            This is particularly relevant in testing where one may be both
        #            firing and listening for events
        database = self.database

        consumer = self.child_transport.consume(
            listen_for=listen_for, consumer_group=consumer_group, **kwargs
        )

        # Wrap the child transport in order to de-duplicate incoming messages
        async for messages in consumer:
            for message in messages:
                await database.start_transaction()

                if await database.is_event_duplicate(message):
                    logger.info(
                        f"Duplicate event {message.canonical_name} detected with ID {message.id}. "
                        f"Skipping."
                    )
                    continue
                else:
                    await database.store_processed_event(message)

                yield [message]

                try:
                    await database.commit_transaction()
                except DuplicateMessage:
                    # TODO: Can this even happen with the appropriate transaction isolation level?
                    logger.info(
                        f"Duplicate event {message.canonical_name} discovered upon commit, "
                        f"will rollback transaction. "
                        f"Duplicates were probably being processed simultaneously, otherwise "
                        f"this would have been caught earlier. Event ID: {message.id}"
                    )
                    await database.rollback_transaction()

    async def publish_pending(self, message_id=None):
        async for message, options in self.database.consume_pending_events(message_id):
            await self.child_transport.send_event(message, options)
            await self.database.remove_pending_event(message.id)


class ConnectionAlreadySet(LightbusException):
    pass


class DatabaseNotSet(LightbusException):
    pass
