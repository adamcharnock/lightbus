from asyncio import AbstractEventLoop

from typing import List, Tuple, Generator

from lightbus.transports.base import get_transport, EventTransport, EventMessage

if False:
    from lightbus.config.structure import EventTransportSelector
    from lightbus.config import Config


class TransactionalEventTransport(EventTransport):
    """ Implement the sending/consumption of events over a given transport.
    """

    def __init__(self, postgres_url: str, child_transport: EventTransport):
        self.postgres_url = postgres_url
        self.child_transport = child_transport

    @classmethod
    def from_config(cls,
                    config: 'Config',
                    postgres_url: str,
                    # TODO: Figure out how to represent this in the config schema
                    #       without circular import problems. May require extracting
                    #       the transport selector structures into json schema 'definitions'
                    # child_transport: 'EventTransportSelector',
                    child_transport: dict
                    ):
        child_transport_config = child_transport._asdict()
        transport_class = get_transport(type_='event', name=child_transport_config.keys()[0])
        return cls(
            postgres_url=postgres_url,
            child_transport=transport_class.from_config(config=config, **child_transport_config)
        )

    def get_existing_connection(self):
        """Get an existing connection, used when sending events"""
        # 1. Get the psycopg connection. Either from Django or explicitly provided in
        #    constructor or send_event()'s options
        pass

    def create_connection(self):
        """Create a new connection, used when fetching and processing events"""
        # 1. Get the psycopg connection. Either from Django or via the provided postgres url
        pass

    async def send_event(self, event_message: EventMessage, options: dict):
        """Publish an event to the bus"""
        # Needs to:
        # 1. Wrap child event transport
        # 2. Store event in db
        # 3. Send events from db once transaction is committed
        pass

    async def fetch(self,
                    listen_for: List[Tuple[str, str]],
                    context: dict,
                    loop: AbstractEventLoop,
                    consumer_group: str=None,
                    **kwargs
                    ) -> Generator[EventMessage, None, None]:
        # Needs to:
        # 1. Wrap child event transport
        # 2. Start transactions
        # 3. Check if message already processed in messages table
        # 4. Insert message into messages table
        # 5. Yield
        # 6. Commit, catching any exceptions relating to 4.
        pass
