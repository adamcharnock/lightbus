import asyncio
import logging
import time
from collections import OrderedDict
from datetime import datetime
from enum import Enum
from typing import Mapping, Optional, List, Tuple, Union, Sequence, AsyncGenerator, Container

from aioredis import ConnectionClosedError, ReplyError
from aioredis.util import decode

from lightbus.transports.base import EventTransport, EventMessage
from lightbus.log import LBullets, L, Bold
from lightbus.serializers import ByFieldMessageSerializer, ByFieldMessageDeserializer
from lightbus.transports.redis.utilities import (
    RedisEventMessage,
    RedisTransportMixin,
    normalise_since_value,
    datetime_to_redis_steam_id,
    redis_stream_id_add_one,
    redis_stream_id_subtract_one,
)
from lightbus.utilities.async_tools import make_exception_checker, cancel
from lightbus.utilities.frozendict import frozendict
from lightbus.utilities.human import human_time
from lightbus.utilities.importing import import_from_string

if False:
    # pylint: disable=unused-import
    from lightbus.config import Config
    from lightbus.client import BusClient


logger = logging.getLogger("lightbus.transports.redis")

Since = Union[str, datetime, None]


class StreamUse(Enum):
    PER_API = "per_api"
    PER_EVENT = "per_event"

    def __eq__(self, other):
        if isinstance(other, str):
            return self.value == other
        else:
            return super().__eq__(other)


class RedisEventTransport(RedisTransportMixin, EventTransport):
    def __init__(
        self,
        redis_pool=None,
        *,
        service_name: str,
        consumer_name: str,
        url=None,
        serializer=ByFieldMessageSerializer(),
        deserializer=ByFieldMessageDeserializer(RedisEventMessage),
        connection_parameters: Mapping = frozendict(maxsize=100),
        batch_size=10,
        reclaim_batch_size: int = None,
        acknowledgement_timeout: float = 60,
        max_stream_length: Optional[int] = 100_000,
        stream_use: StreamUse = StreamUse.PER_API,
        consumption_restart_delay: int = 5,
    ):
        self.set_redis_pool(redis_pool, url, connection_parameters)
        self.batch_size = batch_size
        self.reclaim_batch_size = reclaim_batch_size if reclaim_batch_size else batch_size * 10
        self.service_name = service_name
        self.consumer_name = consumer_name
        self.acknowledgement_timeout = acknowledgement_timeout
        self.max_stream_length = max_stream_length
        self.stream_use = stream_use
        self.consumption_restart_delay = consumption_restart_delay
        super().__init__(serializer=serializer, deserializer=deserializer)

    @classmethod
    def from_config(
        cls,
        config: "Config",
        service_name: str = None,
        consumer_name: str = None,
        url: str = "redis://127.0.0.1:6379/0",
        connection_parameters: Mapping = frozendict(maxsize=100),
        batch_size: int = 10,
        reclaim_batch_size: int = None,
        serializer: str = "lightbus.serializers.ByFieldMessageSerializer",
        deserializer: str = "lightbus.serializers.ByFieldMessageDeserializer",
        acknowledgement_timeout: float = 60,
        max_stream_length: Optional[int] = 100_000,
        stream_use: StreamUse = StreamUse.PER_API,
        consumption_restart_delay: int = 5,
    ):
        serializer = import_from_string(serializer)()
        deserializer = import_from_string(deserializer)(RedisEventMessage)
        service_name = service_name or config.service_name
        consumer_name = consumer_name or config.process_name
        if isinstance(stream_use, str):
            stream_use = StreamUse[stream_use.upper()]

        return cls(
            redis_pool=None,
            service_name=service_name,
            consumer_name=consumer_name,
            url=url,
            connection_parameters=connection_parameters,
            batch_size=batch_size,
            reclaim_batch_size=reclaim_batch_size,
            serializer=serializer,
            deserializer=deserializer,
            acknowledgement_timeout=acknowledgement_timeout,
            max_stream_length=max_stream_length or None,
            stream_use=stream_use,
            consumption_restart_delay=consumption_restart_delay,
        )

    async def send_event(self, event_message: EventMessage, options: dict, bus_client: "BusClient"):
        """Publish an event"""
        stream = self._get_stream_names(
            listen_for=[(event_message.api_name, event_message.event_name)]
        )[0]

        logger.debug(
            LBullets(
                L(
                    "Enqueuing event message {} in Redis stream {}",
                    Bold(event_message),
                    Bold(stream),
                ),
                items=dict(**event_message.get_metadata(), kwargs=event_message.get_kwargs()),
            )
        )

        # Performance: I suspect getting a connection from the connection manager each time is causing
        # performance issues. Need to confirm.
        with await self.connection_manager() as redis:
            start_time = time.time()
            await redis.xadd(
                stream=stream,
                fields=self.serializer(event_message),
                max_len=self.max_stream_length or None,
                exact_len=False,
            )

        logger.debug(
            L(
                "Enqueued event message {} in Redis in {} stream {}",
                Bold(event_message),
                human_time(time.time() - start_time),
                Bold(stream),
            )
        )

    async def consume(
        self,
        listen_for: List[Tuple[str, str]],
        listener_name: str,
        bus_client: "BusClient",
        since: Union[Since, Sequence[Since]] = "$",
        forever=True,
    ) -> AsyncGenerator[List[RedisEventMessage], None]:
        # TODO: Cleanup consumer groups
        self._sanity_check_listen_for(listen_for)

        consumer_group = f"{self.service_name}-{listener_name}"

        if not isinstance(since, (list, tuple)):
            # Since has been specified as a single value. Normalise it into
            # the value-per-listener format.
            since = [since] * len(listen_for)
        since = map(normalise_since_value, since)

        stream_names = self._get_stream_names(listen_for)
        # Keys are stream names, values as the latest ID consumed from that stream
        streams = OrderedDict(zip(stream_names, since))
        expected_events = {event_name for _, event_name in listen_for}

        logger.debug(
            LBullets(
                L(
                    "Consuming events as consumer {} in group {} on streams",
                    Bold(self.consumer_name),
                    Bold(consumer_group),
                ),
                items={"{} ({})".format(*v) for v in streams.items()},
            )
        )

        # Here we use a queue to combine messages coming from both the
        # fetch messages loop and the reclaim messages loop.
        queue = asyncio.Queue(maxsize=1)

        async def consume_loop():
            # Regular event consuming. See _fetch_new_messages()
            while True:
                try:
                    async for messages in self._fetch_new_messages(
                        streams, consumer_group, expected_events, forever
                    ):
                        await queue.put(messages)
                        # Wait for the queue to empty before getting trying to get another message
                        await queue.join()
                except (ConnectionClosedError, ConnectionResetError):
                    # ConnectionClosedError is from aioredis. However, sometimes the connection
                    # can die outside of aioredis, in which case we get a builtin ConnectionResetError.
                    logger.warning(
                        f"Redis connection lost while consuming events, reconnecting "
                        f"in {self.consumption_restart_delay} seconds..."
                    )
                    await asyncio.sleep(self.consumption_restart_delay)

        async def reclaim_loop():
            # Reclaim messages which other consumers have failed to
            # processes in reasonable time. See _reclaim_lost_messages()

            await asyncio.sleep(self.acknowledgement_timeout)
            async for messages in self._reclaim_lost_messages(
                stream_names, consumer_group, expected_events
            ):
                await queue.put(messages)
                # Wait for the queue to empty before getting trying to get another message
                await queue.join()

        consume_task = None
        reclaim_task = None

        try:
            # Run the two above coroutines in their own tasks
            consume_task = asyncio.ensure_future(consume_loop())
            reclaim_task = asyncio.ensure_future(reclaim_loop())

            # Make sure we surface any exceptions that occur in either task
            consume_task.add_done_callback(make_exception_checker(bus_client))
            reclaim_task.add_done_callback(make_exception_checker(bus_client))

            while True:
                try:
                    messages = await queue.get()
                    yield messages
                    queue.task_done()
                except GeneratorExit:
                    return
        finally:
            # Make sure we cleanup the tasks we created
            await cancel(consume_task, reclaim_task)

    async def _fetch_new_messages(
        self, streams, consumer_group, expected_events, forever
    ) -> AsyncGenerator[List[EventMessage], None]:
        """Coroutine to consume new messages

        The consumption has two stages:

          1. Fetch and yield any messages this consumer is responsible for processing but has yet
             to successfully process. This can happen in cases where a message was
             previously consumed but not acknowledged (i.e. due to an error).
             This is a one-off startup stage.
          2. Wait for new messages to arrive. Yield these messages when they arrive, then
             resume waiting for messages

        See Also:

            _reclaim_lost_messages() - Another coroutine which reclaims messages which timed out
                                       while being processed by other consumers in this group

        """
        with await self.connection_manager() as redis:
            # Firstly create the consumer group if we need to
            await self._create_consumer_groups(streams, redis, consumer_group)

            # Get any messages that this consumer has yet to process.
            # This can happen in the case where the processes died before acknowledging.
            pending_messages = await redis.xread_group(
                group_name=consumer_group,
                consumer_name=self.consumer_name,
                streams=list(streams.keys()),
                # Using ID '0' indicates we want unacked pending messages
                latest_ids=["0"] * len(streams),
                timeout=None,  # Don't block, return immediately
            )

            event_messages = []
            for stream, message_id, fields in pending_messages:
                message_id = decode(message_id, "utf8")
                stream = decode(stream, "utf8")
                event_message = self._fields_to_message(
                    fields,
                    expected_events,
                    stream=stream,
                    native_id=message_id,
                    consumer_group=consumer_group,
                )
                if not event_message:
                    # noop message, or message an event we don't care about
                    continue
                logger.debug(
                    LBullets(
                        L(
                            "⬅ Receiving pending event {} on stream {}",
                            Bold(message_id),
                            Bold(stream),
                        ),
                        items=dict(
                            **event_message.get_metadata(), kwargs=event_message.get_kwargs()
                        ),
                    )
                )
                event_messages.append(event_message)

            if event_messages:
                yield event_messages

            # We've now cleaned up any old messages that were hanging around.
            # Now we get on to the main loop which blocks and waits for new messages

            while True:
                # Fetch some messages.
                # This will block until there are some messages available
                stream_messages = await redis.xread_group(
                    group_name=consumer_group,
                    consumer_name=self.consumer_name,
                    streams=list(streams.keys()),
                    # Using ID '>' indicates we only want new messages which have not
                    # been passed to other consumers in this group
                    latest_ids=[">"] * len(streams),
                    count=self.batch_size,
                )

                # Handle the messages we have received
                event_messages = []
                for stream, message_id, fields in stream_messages:
                    message_id = decode(message_id, "utf8")
                    stream = decode(stream, "utf8")
                    event_message = self._fields_to_message(
                        fields,
                        expected_events,
                        stream=stream,
                        native_id=message_id,
                        consumer_group=consumer_group,
                    )
                    if not event_message:
                        # noop message, or message an event we don't care about
                        continue
                    logger.debug(
                        LBullets(
                            L(
                                "⬅ Received new event {} on stream {}",
                                Bold(message_id),
                                Bold(stream),
                            ),
                            items=dict(
                                **event_message.get_metadata(), kwargs=event_message.get_kwargs()
                            ),
                        )
                    )
                    # NOTE: YIELD ALL MESSAGES, NOT JUST ONE
                    event_messages.append(event_message)

                if event_messages:
                    yield event_messages

                if not forever:
                    return

    async def _reclaim_lost_messages(
        self, stream_names: List[str], consumer_group: str, expected_events: set
    ) -> AsyncGenerator[List[EventMessage], None]:
        """Reclaim messages that other consumers in the group failed to acknowledge within a timeout

        The timeout period is specified by the `acknowledgement_timeout` option.
        """
        with await self.connection_manager() as redis:
            for stream in stream_names:
                old_messages = await redis.xpending(
                    stream, consumer_group, "-", "+", count=self.reclaim_batch_size
                )
                timeout = self.acknowledgement_timeout * 1000
                event_messages = []
                for (
                    message_id,
                    consumer_name,
                    ms_since_last_delivery,
                    num_deliveries,
                ) in old_messages:
                    message_id = decode(message_id, "utf8")
                    consumer_name = decode(consumer_name, "utf8")

                    if ms_since_last_delivery > timeout:
                        logger.info(
                            L(
                                "Found timed out event {} in stream {}. Abandoned by {}. Attempting to reclaim...",
                                Bold(message_id),
                                Bold(stream),
                                Bold(consumer_name),
                            )
                        )

                    result = await redis.xclaim(
                        stream, consumer_group, self.consumer_name, int(timeout), message_id
                    )
                    for claimed_message_id, fields in result:
                        claimed_message_id = decode(claimed_message_id, "utf8")
                        event_message = self._fields_to_message(
                            fields,
                            expected_events,
                            stream=stream,
                            native_id=claimed_message_id,
                            consumer_group=consumer_group,
                        )
                        if not event_message:
                            # noop message, or message an event we don't care about
                            continue
                        logger.debug(
                            LBullets(
                                L(
                                    "⬅ Reclaimed timed out event {} on stream {}. Abandoned by {}.",
                                    Bold(message_id),
                                    Bold(stream),
                                    Bold(consumer_name),
                                ),
                                items=dict(
                                    **event_message.get_metadata(),
                                    kwargs=event_message.get_kwargs(),
                                ),
                            )
                        )
                        event_messages.append(event_message)

                    if event_messages:
                        yield event_messages

    async def acknowledge(self, *event_messages: RedisEventMessage, bus_client: "BusClient"):
        with await self.connection_manager() as redis:
            p = redis.pipeline()
            for event_message in event_messages:
                p.xack(event_message.stream, event_message.consumer_group, event_message.native_id)
                logging.debug(
                    f"Preparing to acknowledge message {event_message.id} (Native ID: {event_message.native_id})"
                )

            logger.debug(
                f"Batch acknowledging successful processing of {len(event_messages)} message."
            )
            await p.execute()

    async def history(
        self,
        api_name,
        event_name,
        start: datetime = None,
        stop: datetime = None,
        start_inclusive: bool = True,
        batch_size: int = 100,
    ) -> AsyncGenerator[EventMessage, None]:
        # TODO: Test
        redis_start = datetime_to_redis_steam_id(start) if start else "-"
        redis_stop = datetime_to_redis_steam_id(stop) if stop else "+"

        if start and not start_inclusive:
            redis_start = redis_stream_id_add_one(redis_start)

        stream_name = self._get_stream_names([(api_name, event_name)])[0]

        logger.debug(
            f"Getting history for stream {stream_name} from {redis_start} ({start}) "
            f"to {redis_stop} ({stop}) in batches of {batch_size}"
        )

        with await self.connection_manager() as redis:
            messages = True
            while messages:
                messages = await redis.xrevrange(
                    stream_name, redis_stop, redis_start, count=batch_size
                )
                if not messages:
                    return
                for message_id, fields in messages:
                    message_id = decode(message_id, "utf8")
                    redis_stop = redis_stream_id_subtract_one(message_id)
                    event_message = self._fields_to_message(
                        fields,
                        expected_event_names={event_name},
                        stream=stream_name,
                        native_id=message_id,
                        consumer_group=None,
                    )
                    if event_message:
                        yield event_message

    async def _create_consumer_groups(self, streams, redis, consumer_group):
        for stream, since in streams.items():
            if not await redis.exists(stream):
                # Add a noop to ensure the stream exists
                await redis.xadd(stream, fields={"": ""})

            try:
                # Create the group (it may already exist)
                await redis.xgroup_create(stream, consumer_group, latest_id=since)
            except ReplyError as e:
                if "BUSYGROUP" not in str(e):
                    raise

    def _fields_to_message(
        self,
        fields: dict,
        expected_event_names: Container[str],
        stream: str,
        native_id: str,
        consumer_group: Optional[str],
    ) -> Optional[RedisEventMessage]:
        if tuple(fields.items()) == ((b"", b""),):
            return None
        message = self.deserializer(
            fields, stream=stream, native_id=native_id, consumer_group=consumer_group
        )

        want_message = ("*" in expected_event_names) or (message.event_name in expected_event_names)
        if self.stream_use == StreamUse.PER_API and not want_message:
            # Only care about events we are listening for. If we have one stream
            # per API then we're probably going to receive some events we don't care about.
            logger.debug(f"Ignoring message for unexpected event: {message}")
            return None
        return message

    def _get_stream_names(self, listen_for):
        """Convert a list of api names & event names into stream names

        The format of these names will vary based on the stream_use setting.
        """
        stream_names = []
        for api_name, event_name in listen_for:
            if self.stream_use == StreamUse.PER_EVENT:
                stream_name = f"{api_name}.{event_name}:stream"
            elif self.stream_use == StreamUse.PER_API:
                stream_name = f"{api_name}.*:stream"
            else:
                raise ValueError(
                    "Invalid value for stream_use config option. This should have been caught "
                    "during config validation."
                )
            if stream_name not in stream_names:
                stream_names.append(stream_name)
        return stream_names
