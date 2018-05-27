import json
from functools import partial
from typing import Tuple, Optional, AsyncIterable, AsyncGenerator

from lightbus.exceptions import UnsupportedOptionValue
from lightbus.schema.encoder import json_encode
from lightbus.serializers import BlobMessageSerializer, BlobMessageDeserializer
from lightbus.transports.base import EventMessage

try:
    import django
except ImportError:
    django = None


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


#
# class DjangoConnection(DbApiConnection):
#
#     @classmethod
#     async def create(cls: Type[T]) -> T:
#         if not django:
#             raise DjangoNotInstalled(
#                 "You are trying to use the Django database for you transactional event transport, "
#                 "but django could not be imported. Is Django installed? Or perhaps you don't really "
#                 "want to be using the Django database?"
#             )
#
#         # Get the underlying django psycopg2 connection
#         return DjangoConnection(connection)
#
#     # ... all the other methods. Need to consult django docs...
#
#
# class DjangoNotInstalled(LightbusException):
#     pass
