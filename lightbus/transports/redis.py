import asyncio
import json
import logging
import time
from collections import OrderedDict
from datetime import datetime
from typing import (
    Sequence,
    Optional,
    Union,
    Generator,
    Dict,
    Mapping,
    List,
    Container,
    AsyncGenerator,
)
from enum import Enum

import aioredis
from aioredis import Redis, ReplyError, ConnectionClosedError
from aioredis.pool import ConnectionsPool
from aioredis.util import decode

from lightbus.api import Api
from lightbus.exceptions import LightbusException, LightbusShutdownInProgress, TransportIsClosed
from lightbus.log import L, Bold, LBullets
from lightbus.message import RpcMessage, ResultMessage, EventMessage
from lightbus.schema.encoder import json_encode
from lightbus.serializers.blob import BlobMessageSerializer, BlobMessageDeserializer
from lightbus.serializers.by_field import ByFieldMessageSerializer, ByFieldMessageDeserializer
from lightbus.transports.base import ResultTransport, RpcTransport, EventTransport, SchemaTransport
from lightbus.utilities.async_tools import cancel, check_for_exception
from lightbus.utilities.frozendict import frozendict
from lightbus.utilities.human import human_time
from lightbus.utilities.importing import import_from_string

if False:
    # pylint: disable=unused-import
    from lightbus.config import Config

logger = logging.getLogger(__name__)

Since = Union[str, datetime, None]


class StreamUse(Enum):
    PER_API = "per_api"
    PER_EVENT = "per_event"

    def __eq__(self, other):
        if isinstance(other, str):
            return self.value == other
        else:
            return super().__eq__(other)


class RedisEventMessage(EventMessage):
    def __init__(self, *, stream: str, native_id: str, consumer_group: str, **kwargs):
        super(RedisEventMessage, self).__init__(**kwargs)
        self.stream = stream
        self.native_id = native_id
        self.consumer_group = consumer_group


class RedisTransportMixin(object):
    connection_parameters: dict = {"address": "redis://localhost:6379", "maxsize": 100}
    _redis_pool = None

    def set_redis_pool(
        self,
        redis_pool: Optional[Redis],
        url: str = None,
        connection_parameters: Mapping = frozendict(),
    ):
        self._redis_pool = None
        self._closed = False

        if not redis_pool:
            # Connect lazily using the provided parameters

            self.connection_parameters = self.connection_parameters.copy()
            self.connection_parameters.update(connection_parameters)
            if url:
                self.connection_parameters["address"] = url
        else:
            # Use the provided connection

            if isinstance(redis_pool, (ConnectionsPool,)):
                # If they've passed a raw pool then wrap it up in a Redis object.
                # aioredis.create_redis_pool() normally does this for us.
                redis_pool = Redis(redis_pool)
            if not isinstance(redis_pool, (Redis,)):
                raise InvalidRedisPool(
                    "Invalid Redis connection provided: {}. If unsure, use aioredis.create_redis_pool() to "
                    "create your redis connection.".format(redis_pool)
                )
            if not isinstance(redis_pool._pool_or_conn, (ConnectionsPool,)):
                raise InvalidRedisPool(
                    "The provided redis connection is backed by a single connection, rather than a "
                    "pool of connections. This will lead to lightbus deadlocks and is unsupported. "
                    "If unsure, use aioredis.create_redis_pool() to create your redis connection."
                )

            # Determine the connection parameters from the given pool
            # (we will need these in other to create new pools for other threads)
            self.connection_parameters = dict(
                address=redis_pool.address,
                db=redis_pool.db,
                password=redis_pool._pool_or_conn._password,
                encoding=redis_pool.encoding,
                minsize=redis_pool._pool_or_conn.minsize,
                maxsize=redis_pool._pool_or_conn.maxsize,
                ssl=redis_pool._pool_or_conn._ssl,
                parser=redis_pool._pool_or_conn._parser_class,
                timeout=redis_pool._pool_or_conn._create_connection_timeout,
                connection_cls=redis_pool._pool_or_conn._connection_cls,
            )

            self._redis_pool = redis_pool

    async def connection_manager(self) -> Redis:
        if self._closed:
            # This was first caught when the state plugin tried to send a
            # message to the bus on upon the after_server_stopped stopped event.
            raise TransportIsClosed(
                "Transport has been closed. Connection to Redis is no longer available."
            )

        if not self._redis_pool:
            self._redis_pool = await aioredis.create_redis_pool(**self.connection_parameters)
        try:
            internal_pool = self._redis_pool._pool_or_conn
            if hasattr(internal_pool, "size") and hasattr(internal_pool, "maxsize"):
                if internal_pool.size == internal_pool.maxsize:
                    logging.critical(
                        "Redis pool has reached maximum size. It is possible that this will recover normally, "
                        "but may be you have more event listeners than connections available to the Redis pool. "
                        "You can increase the redis pool size by specifying the `maxsize` "
                        "parameter in each of the Redis transport configuration sections. Current maxsize is: {}"
                        "".format(self.connection_parameters.get("maxsize"))
                    )

            return await self._redis_pool
        except aioredis.PoolClosedError:
            raise LightbusShutdownInProgress(
                "Redis connection pool has been closed. Assuming shutdown in progress."
            )

    async def close(self):
        if self._redis_pool:
            await self._close_redis_pool()
        self._closed = True

    async def _close_redis_pool(self):
        self._redis_pool.close()
        await self._redis_pool.wait_closed()
        # In Python >= 3.7 the redis connections do not actually get
        # immediately closed following the above call to wait_closed().
        # If you need to be sure the the connections have closed the
        # a call to `await asyncio.sleep(0.001)` does the trick
        del self._redis_pool

    def __str__(self):
        if hasattr(self._local, "redis_pool"):
            conn = self._redis_pool.connection
            return f"redis://{conn.address[0]}:{conn.address[1]}/{conn.db}"
        else:
            return self.connection_parameters.get("address", "Unknown URL")


class RedisRpcTransport(RedisTransportMixin, RpcTransport):
    """ Redis RPC transport providing at-most-once delivery

    This transport uses a redis list and a blocking pop operation
    to distribute an RPC call to a single RPC consumer.

    Each call also has a corresponding expiry key created. Once the
    key expires it should be assumed that the RPC call has timed
    out and that therefore is should be discarded rather than
    be processed.
    """

    def __init__(
        self,
        *,
        redis_pool=None,
        url=None,
        serializer=BlobMessageSerializer(),
        deserializer=BlobMessageDeserializer(RpcMessage),
        connection_parameters: Mapping = frozendict(maxsize=100),
        batch_size=10,
        rpc_timeout=5,
        consumption_restart_delay=5,
    ):
        self.set_redis_pool(redis_pool, url, connection_parameters)
        self._latest_ids = {}
        self.serializer = serializer
        self.deserializer = deserializer
        self.batch_size = batch_size
        self.rpc_timeout = rpc_timeout
        self.consumption_restart_delay = consumption_restart_delay

    @classmethod
    def from_config(
        cls,
        config: "Config",
        url: str = "redis://127.0.0.1:6379/0",
        connection_parameters: Mapping = frozendict(maxsize=100),
        batch_size: int = 10,
        serializer: str = "lightbus.serializers.BlobMessageSerializer",
        deserializer: str = "lightbus.serializers.BlobMessageDeserializer",
        rpc_timeout: int = 5,
        consumption_restart_delay: int = 5,
    ):
        serializer = import_from_string(serializer)()
        deserializer = import_from_string(deserializer)(RpcMessage)

        return cls(
            url=url,
            serializer=serializer,
            deserializer=deserializer,
            connection_parameters=connection_parameters,
            batch_size=batch_size,
            rpc_timeout=rpc_timeout,
            consumption_restart_delay=consumption_restart_delay,
        )

    async def call_rpc(self, rpc_message: RpcMessage, options: dict):
        queue_key = f"{rpc_message.api_name}:rpc_queue"
        expiry_key = f"rpc_expiry_key:{rpc_message.id}"
        logger.debug(
            LBullets(
                L("Enqueuing message {} in Redis stream {}", Bold(rpc_message), Bold(queue_key)),
                items=dict(**rpc_message.get_metadata(), kwargs=rpc_message.get_kwargs()),
            )
        )

        with await self.connection_manager() as redis:
            start_time = time.time()
            p = redis.pipeline()
            p.rpush(key=queue_key, value=self.serializer(rpc_message))
            p.set(expiry_key, 1)
            # TODO: Ditch this somehow. We kind of need the ApiConfig here, but a transports can be
            #       used for multiple APIs. So we could pass it in for each use of the transports,
            #       but even then it doesn't work for the event listening. This is because an event listener
            #       can span multiple APIs
            p.expire(expiry_key, timeout=self.rpc_timeout)
            await p.execute()

        logger.debug(
            L(
                "Enqueued message {} in Redis in {} stream {}",
                Bold(rpc_message),
                human_time(time.time() - start_time),
                Bold(queue_key),
            )
        )

    async def consume_rpcs(self, apis: Sequence[Api]) -> Sequence[RpcMessage]:
        while True:
            try:
                return await self._consume_rpcs(apis)
            except (ConnectionClosedError, ConnectionResetError):
                # ConnectionClosedError is from aioredis. However, sometimes the connection
                # can die outside of aioredis, in which case we get a builtin ConnectionResetError.
                logger.warning(
                    f"Redis connection lost while consuming RPCs, reconnecting "
                    f"in {self.consumption_restart_delay} seconds..."
                )
                await asyncio.sleep(self.consumption_restart_delay)

    async def _consume_rpcs(self, apis: Sequence[Api]) -> Sequence[RpcMessage]:
        # Get the name of each stream
        queue_keys = ["{}:rpc_queue".format(api.meta.name) for api in apis]

        logger.debug(
            LBullets(
                "Consuming RPCs from",
                items=["{} ({})".format(s, self._latest_ids.get(s, "$")) for s in queue_keys],
            )
        )

        with await self.connection_manager() as redis:
            try:
                stream, data = await redis.blpop(*queue_keys)
            except RuntimeError:
                # For some reason aio-redis likes to eat the CancelledError and
                # turn it into a Runtime error:
                # https://github.com/aio-libs/aioredis/blob/9f5964/aioredis/connection.py#L184
                raise asyncio.CancelledError(
                    "aio-redis task was cancelled and decided it should be a RuntimeError"
                )

            stream = decode(stream, "utf8")
            rpc_message = self.deserializer(data)
            expiry_key = f"rpc_expiry_key:{rpc_message.id}"
            key_deleted = await redis.delete(expiry_key)

            if not key_deleted:
                return []

            logger.debug(
                LBullets(
                    L("⬅ Received RPC message on stream {}", Bold(stream)),
                    items=dict(**rpc_message.get_metadata(), kwargs=rpc_message.get_kwargs()),
                )
            )

            return [rpc_message]


class RedisResultTransport(RedisTransportMixin, ResultTransport):
    def __init__(
        self,
        *,
        redis_pool=None,
        url=None,
        serializer=BlobMessageSerializer(),
        deserializer=BlobMessageDeserializer(ResultMessage),
        connection_parameters: Mapping = frozendict(maxsize=100),
        result_ttl=60,
        rpc_timeout=5,
    ):
        # NOTE: We use the blob message_serializer here, as the results come back as values in a list
        self.set_redis_pool(redis_pool, url, connection_parameters)
        self.serializer = serializer
        self.deserializer = deserializer
        self.result_ttl = result_ttl
        self.rpc_timeout = rpc_timeout

    @classmethod
    def from_config(
        cls,
        config: "Config",
        url: str = "redis://127.0.0.1:6379/0",
        serializer: str = "lightbus.serializers.BlobMessageSerializer",
        deserializer: str = "lightbus.serializers.BlobMessageDeserializer",
        connection_parameters: Mapping = frozendict(maxsize=100),
        result_ttl=60,
        rpc_timeout=5,
    ):
        serializer = import_from_string(serializer)()
        deserializer = import_from_string(deserializer)(ResultMessage)

        return cls(
            url=url,
            serializer=serializer,
            deserializer=deserializer,
            connection_parameters=connection_parameters,
            result_ttl=result_ttl,
            rpc_timeout=rpc_timeout,
        )

    def get_return_path(self, rpc_message: RpcMessage) -> str:
        return "redis+key://{}.{}:result:{}".format(
            rpc_message.api_name, rpc_message.procedure_name, rpc_message.id
        )

    async def send_result(
        self, rpc_message: RpcMessage, result_message: ResultMessage, return_path: str
    ):
        logger.debug(
            L(
                "Sending result {} into Redis using return path {}",
                Bold(result_message),
                Bold(return_path),
            )
        )
        redis_key = self._parse_return_path(return_path)

        with await self.connection_manager() as redis:
            start_time = time.time()
            p = redis.pipeline()
            p.lpush(redis_key, self.serializer(result_message))
            p.expire(redis_key, timeout=self.result_ttl)
            await p.execute()

        logger.debug(
            L(
                "➡ Sent result {} into Redis in {} using return path {}",
                Bold(result_message),
                human_time(time.time() - start_time),
                Bold(return_path),
            )
        )

    async def receive_result(
        self, rpc_message: RpcMessage, return_path: str, options: dict
    ) -> ResultMessage:
        logger.debug(L("Awaiting Redis result for RPC message: {}", Bold(rpc_message)))
        redis_key = self._parse_return_path(return_path)

        with await self.connection_manager() as redis:
            start_time = time.time()
            result = None
            while not result:
                # Sometimes blpop() will return None in the case of timeout or
                # cancellation. We therefore perform this step with a loop to catch
                # this. A more elegant solution is welcome.
                # TODO: RPC & result Transports should not be applying a timeout, leave
                #       this to the client which coordinates between the two
                result = await redis.blpop(redis_key, timeout=self.rpc_timeout)
            _, serialized = result

        result_message = self.deserializer(serialized)

        logger.debug(
            L(
                "⬅ Received Redis result in {} for RPC message {}: {}",
                human_time(time.time() - start_time),
                rpc_message,
                Bold(result_message.result),
            )
        )

        return result_message

    def _parse_return_path(self, return_path: str) -> str:
        if not return_path.startswith("redis+key://"):
            raise AssertionError(f"Invalid return path specified: {return_path}")
        return return_path[12:]


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
        self.serializer = serializer
        self.deserializer = deserializer
        self.batch_size = batch_size
        self.reclaim_batch_size = reclaim_batch_size if reclaim_batch_size else batch_size * 10
        self.service_name = service_name
        self.consumer_name = consumer_name
        self.acknowledgement_timeout = acknowledgement_timeout
        self.max_stream_length = max_stream_length
        self.stream_use = stream_use
        self.consumption_restart_delay = consumption_restart_delay

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

    async def send_event(self, event_message: EventMessage, options: dict):
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
        listen_for,
        listener_name: str,
        since: Union[Since, Sequence[Since]] = "$",
        forever=True,
    ) -> AsyncGenerator[List[RedisEventMessage], None]:
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
            consume_task.add_done_callback(check_for_exception)
            reclaim_task.add_done_callback(check_for_exception)

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

    async def close(self):
        await super().close()

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

    async def acknowledge(self, *event_messages: RedisEventMessage):
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
        consumer_group: str,
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


class RedisSchemaTransport(RedisTransportMixin, SchemaTransport):
    def __init__(
        self,
        *,
        redis_pool=None,
        url: str = "redis://127.0.0.1:6379/0",
        connection_parameters: Mapping = frozendict(),
    ):
        self.set_redis_pool(redis_pool, url, connection_parameters)
        self._latest_ids = {}

    @classmethod
    def from_config(
        cls,
        config,
        url: str = "redis://127.0.0.1:6379/0",
        connection_parameters: Mapping = frozendict(),
    ):
        return cls(url=url, connection_parameters=connection_parameters)

    def schema_key(self, api_name):
        return "schema:{}".format(api_name)

    def schema_set_key(self):
        """Maintains a set of api names in redis which can be used to retrieve individual schemas"""
        return "schemas"

    async def store(self, api_name: str, schema: Dict, ttl_seconds: Optional[int]):
        """Store an individual schema"""
        with await self.connection_manager() as redis:
            schema_key = self.schema_key(api_name)

            p = redis.pipeline()
            p.set(schema_key, json_encode(schema))
            if ttl_seconds is not None:
                p.expire(schema_key, ttl_seconds)
            p.sadd(self.schema_set_key(), api_name)
            await p.execute()

    async def load(self) -> Dict[str, Dict]:
        """Load all schemas"""
        schemas = {}
        with await self.connection_manager() as redis:
            # Get & decode the api names
            api_names = list(await redis.smembers(self.schema_set_key()))
            api_names = [api_name.decode("utf8") for api_name in api_names]

            # Convert the api names into redis keys
            keys = [self.schema_key(api_name) for api_name in api_names]

            if not keys:
                return {}

            # Get the schemas from the keys
            encoded_schemas = await redis.mget(*keys)
            for api_name, schema in zip(api_names, encoded_schemas):
                # Schema may have expired
                if schema:
                    schemas[api_name] = json.loads(schema)
        return schemas


def redis_stream_id_subtract_one(message_id):
    """Subtract one from the message ID

    This is useful when we need to xread() events inclusive of the given ID,
    rather than exclusive of the given ID (which is the sensible default).
    Only use when one can tolerate the slim risk of grabbing extra events.
    """
    milliseconds, n = map(int, message_id.split("-"))
    if n > 0:
        n = n - 1
    elif milliseconds > 0:
        milliseconds = milliseconds - 1
        n = 9999
    else:
        # message_id is '0000000000000-0'. Subtracting one
        # from this is neither possible, desirable or useful.
        return message_id
    return "{:13d}-{}".format(milliseconds, n)


def normalise_since_value(since):
    """Take a 'since' value and normalise it to be a redis message ID"""
    if not since:
        return "$"
    elif hasattr(since, "timestamp"):  # datetime
        # Create message ID: "<milliseconds-timestamp>-<sequence-number>"
        return "{}-0".format(round(since.timestamp() * 1000))
    else:
        return since


def redis_steam_id_to_datetime(message_id):
    message_id = decode(message_id, "utf8")
    milliseconds, seq = map(int, message_id.split("-"))
    # Treat the sequence value as additional microseconds to ensure correct sequencing
    microseconds = (milliseconds % 1000 * 1000) + seq
    dt = datetime.utcfromtimestamp(milliseconds // 1000).replace(microsecond=microseconds)
    return dt


class InvalidRedisPool(LightbusException):
    pass
