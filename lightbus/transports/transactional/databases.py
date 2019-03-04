import json
from functools import partial
from inspect import isawaitable
from typing import Tuple, Optional, AsyncIterable, AsyncGenerator

from lightbus.exceptions import UnsupportedOptionValue, DuplicateMessage
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

    async def migrate(self):
        raise NotImplementedError()

    async def start_transaction(self):
        raise NotImplementedError()

    async def commit_transaction(self):
        # Should raise DuplicateMessage() if unique key on de-duping table is violated
        raise NotImplementedError()

    async def rollback_transaction(self):
        raise NotImplementedError()

    async def is_event_duplicate(self, message: EventMessage, listener_name: str) -> bool:
        raise NotImplementedError()

    async def store_processed_event(self, message: EventMessage, listener_name: str):
        # Store message in de-duping table
        # Should raise DuplicateMessage() if message is duplicate
        raise NotImplementedError()

    async def send_event(self, message: EventMessage, options: dict):
        # Store message in messages-to-be-published table
        raise NotImplementedError()

    async def consume_pending_events(
        self, message_id: Optional[str] = None
    ) -> AsyncGenerator[Tuple[EventMessage, dict], None]:
        # Should lock row in db
        raise NotImplementedError()

    async def remove_pending_event(self, message_id):
        raise NotImplementedError()


class DbApiConnection(DatabaseConnection):
    async def migrate(self):
        # TODO: smarter version handling
        # TODO: cleanup old entries after x time

        # Note: This may will fail with an integrity error if simultaneously
        #       executed multiple times, as per:
        #       https://stackoverflow.com/questions/29900845/

        await self.execute(
            """
            CREATE TABLE IF NOT EXISTS lightbus_processed_events (
                message_id VARCHAR(100),
                listener_name VARCHAR(200),
                processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                PRIMARY KEY(message_id, listener_name)
            )
        """
        )

        await self.execute(
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
        await self.execute(
            "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED -- DbApiConnection.start_transaction()"
        )

    async def commit_transaction(self):
        try:
            # We do this manually because self.connection.commit() will not work as per:
            #   http://initd.org/psycopg/docs/advanced.html#asynchronous-support
            await self.execute("COMMIT -- DbApiConnection.commit_transaction()")
        except Exception as e:
            error_name = e.__class__.__name__.lower()
            error = str(e).lower()
            # A bit of a fudge to detect duplicate events at commit time.
            # Cannot use actual exception as we do not know which client
            # library will be used
            # TODO: Can this even happen with the appropriate transaction isolation level?
            if "integrity" in error_name and "lightbus_processed_events" in error:
                raise DuplicateMessage(
                    "Duplicate event detected upon commit. We normally catch this "
                    "preemptively, but it is possible that duplicate messages were "
                    "being processed in parallel, in which case we only catch it "
                    "upon commit."
                )
            else:
                raise

    async def rollback_transaction(self):
        # We do this manually because self.connection.rollback() will not work as per:
        #   http://initd.org/psycopg/docs/advanced.html#asynchronous-support
        await self.execute("ROLLBACK -- DbApiConnection.rollback_transaction()")

    async def is_event_duplicate(self, message: EventMessage, listener_name: str) -> bool:
        sql = "SELECT EXISTS(SELECT 1 FROM lightbus_processed_events WHERE message_id = %s AND listener_name = %s)"
        await self.execute(sql, [message.id, listener_name])
        result = await self.fetchall()
        return result[0][0]

    async def store_processed_event(self, message: EventMessage, listener_name: str):
        # Store message in de-duping table
        sql = "INSERT INTO lightbus_processed_events (message_id, listener_name) VALUES (%s, %s)"
        await self.execute(sql, [message.id, listener_name])

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
        await self.execute(sql, [message.id, self.message_serializer(message), encoded_options])

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
            sql = "SELECT message, options FROM lightbus_event_outbox {} LIMIT 1 FOR UPDATE SKIP LOCKED".format(  # nosec
                where
            )
            await self.execute(sql, args)
            result = await self.fetchall()

            # test-hook: pending_post_fetch

            if not result:
                return
            else:
                message, options = result[0]
                deserialized_message = self.message_deserializer(message)
                deserialized_options = self.options_deserializer(options) if options else {}
                yield (deserialized_message, deserialized_options)

    async def remove_pending_event(self, message_id):
        sql = "DELETE FROM lightbus_event_outbox WHERE message_id = %s"
        await self.execute(sql, [message_id])

    async def execute(self, sql, args=None):
        result = self.cursor.execute(sql, args or [])
        if isawaitable(result):
            result = await result
        return result

    async def fetchall(self):
        result = self.cursor.fetchall()
        if isawaitable(result):
            result = await result
        return result


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
