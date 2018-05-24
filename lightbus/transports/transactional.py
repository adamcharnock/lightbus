import logging
from asyncio import AbstractEventLoop

from typing import List, Tuple, Generator, Type, TypeVar, Optional, Sequence, AsyncIterable

from lightbus.exceptions import LightbusException
from lightbus.transports.base import get_transport, EventTransport, EventMessage
from lightbus.utilities.importing import import_from_string

try:
    import django
    DJANGO_AVAILABLE = True
except ImportError:
    DJANGO_AVAILABLE = False

if False:
    from lightbus.config.structure import EventTransportSelector
    from lightbus.config import Config

logger = logging.getLogger(__name__)


class TransactionalEventTransport(EventTransport):
    """ Implement the sending/consumption of events over a given transport.
    """

    def __init__(self,
                 postgres_url: str,
                 child_transport: EventTransport,
                 database_class: Type['DatabaseConnection'],
                 ):
        self.postgres_url = postgres_url
        self.child_transport = child_transport
        self.database_class = database_class
        self.database: Optional['DatabaseConnection'] = None

    @classmethod
    def from_config(cls,
                    config: 'Config',
                    postgres_url: str,
                    # TODO: Figure out how to represent child_transport in the config schema
                    #       without circular import problems. May require extracting
                    #       the transport selector structures into json schema 'definitions'
                    # child_transport: 'EventTransportSelector',
                    child_transport: dict,
                    database_class: str='lightbus.transports.transactional.FooConnection'
                    ):
        child_transport_config = child_transport._asdict()
        transport_class = get_transport(type_='event', name=child_transport_config.keys()[0])
        return cls(
            postgres_url=postgres_url,
            child_transport=transport_class.from_config(config=config, **child_transport_config),
            database_class=import_from_string(database_class)
        )

    async def open(self):
        self.database = await self.database_class.create()

    async def send_event(self, event_message: EventMessage, options: dict):
        assert self.database, 'Transport has not been opened yet'

        await self.database.send_event(event_message, options)
        await self.publish_pending()  # TODO: Specific ID

    async def fetch(self,
                    listen_for: List[Tuple[str, str]],
                    context: dict,
                    loop: AbstractEventLoop,
                    consumer_group: str=None,
                    **kwargs
                    ) -> Generator[EventMessage, None, None]:
        assert self.database, 'Transport has not been opened yet'

        # Needs to:
        # 1. Start transactions
        # 2. Check if message already processed in messages table
        # 3. Insert message into messages table
        # 4. Yield
        # 5. Commit, catching any exceptions relating to 3.

        consumer = self.child_transport.consume(
            listen_for=listen_for,
            context=context,
            loop=loop,
            consumer_group=consumer_group,
            **kwargs
        )
        for message in consumer:
            try:
                await self.database.start_transaction()

                # Catch duplicate events up-front where possible, rather than
                # perform the processing and get an integrity error later
                if await self.database.is_event_duplicate(message):
                    logger.info(f'Duplicate event detected, ignoring. Event ID: {message.id}')
                    continue

                self.database.store_processed_event(message)
                yield message
            except Exception:
                logger.warning(
                    f'Error encountered while processing event ID {message.id}. Rolling back transaction.'
                )
                await self.database.rollback_transaction()
                raise
            else:
                try:
                    await self.database.commit_transaction()
                except DuplicateMessage:
                    logger.info(
                        f'Duplicate event discovered upon commit, will rollback transaction. '
                        f'Duplicates were probably being processed simultaneously, otherwise '
                        f'this would have been caught earlier. Event ID: {message.id}'
                    )
                    await self.database.rollback_transaction()

    async def publish_pending(self):
        async for message, options in self.database.consume_pending_events():
            await self.child_transport.send_event(message, options)
            await self.database.remove_pending_event(message.id)


T = TypeVar('T')


class DatabaseConnection(object):

    @staticmethod
    async def create(cls: Type[T]) -> T:
        raise NotImplementedError()

    async def in_transaction(self) -> bool:
        raise NotImplementedError()

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

    async def consume_pending_events(self, message_id: Optional[str]=None) -> AsyncIterable[Tuple[EventMessage, dict]]:
        # Should lock row in db
        raise NotImplementedError()

    async def remove_pending_event(self, message_id):
        raise NotImplementedError()


class DuplicateMessage(Exception): pass


class Pyscopg2Connection(DatabaseConnection):

    def __init__(self, connection: Foo):
        self.connection = connection


class DjangoConnection(Pyscopg2Connection):

    @classmethod
    async def create(cls: Type[T]) -> T:
        if not DJANGO_AVAILABLE:
            raise DjangoNotInstalled(
                "You are trying to use the Django database for you transactional event transport, "
                "but django could not be imported. Is Django installed? Or perhaps you don't really "
                "want to be using the Django database?"
            )
        import django.db.connection
        # Get the underlying django psycopg2 connection
        return DjangoConnection(connection)

    # ... all the other methods. Need to consult django docs...


class DjangoNotInstalled(LightbusException): pass
