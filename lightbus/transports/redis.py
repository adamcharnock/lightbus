import json
import logging
import time
from datetime import datetime
from typing import Sequence, Optional, Union, Generator, Dict

import aioredis
import asyncio
from aioredis import Redis
from aioredis.pool import ConnectionsPool
from aioredis.util import decode
from collections import OrderedDict

from lightbus.api import Api
from lightbus.exceptions import LightbusException, LightbusShutdownInProgress
from lightbus.log import L, Bold, LBullets
from lightbus.message import RpcMessage, ResultMessage, EventMessage
from lightbus.serializers.blob import BlobMessageSerializer, BlobMessageDeserializer
from lightbus.serializers.by_field import ByFieldMessageSerializer, ByFieldMessageDeserializer
from lightbus.transports.base import ResultTransport, RpcTransport, EventTransport, SchemaTransport
from lightbus.utilities import human_time

logger = logging.getLogger(__name__)

# TODO: There is a lot of duplicated code here, particularly between the RPC transport & event transport

Since = Union[str, datetime, None]


class RedisTransportMixin(object):
    connection_kwargs: dict = {
        'address': ('localhost', 6379),
        'maxsize': 100,
    }
    _redis_pool: Optional[Redis] = None

    def set_redis_pool(self, redis_pool: Optional[Redis]):
        if redis_pool:
            if isinstance(redis_pool, (ConnectionsPool,)):
                # If they've passed a raw pool then wrap it up in a Redis object.
                # aioredis.create_redis_pool() normally does this for us.
                redis_pool = Redis(redis_pool)
            if not isinstance(redis_pool, (Redis,)):
                raise InvalidRedisPool(
                    'Invalid Redis connection provided: {}. If unsure, use aioredis.create_redis_pool() to '
                    'create your redis connection.'.format(redis_pool)
                )
            if not isinstance(redis_pool._pool_or_conn, (ConnectionsPool,)):
                raise InvalidRedisPool(
                    'The provided redis connection is backed by a single connection, rather than a '
                    'pool of connections. This will lead to lightbus deadlocks and is unsupported. '
                    'If unsure, use aioredis.create_redis_pool() to create your redis connection.'
                )

            self._redis_pool = redis_pool

    def set_connection_kwargs(self, connection_kwargs: dict):
        # Apply sensible default values from above
        connection_kwargs = connection_kwargs.copy()
        connection_kwargs.update(**self.connection_kwargs)
        self.connection_kwargs = connection_kwargs

    async def connection_manager(self) -> Redis:
        if self._redis_pool is None:
            self._redis_pool = await aioredis.create_redis_pool(**self.connection_kwargs)
        try:
            internal_pool = self._redis_pool._pool_or_conn
            if hasattr(internal_pool, 'size') and hasattr(internal_pool, 'maxsize'):
                if internal_pool.size == internal_pool.maxsize:
                    logging.critical(
                        "Redis pool has reached maximum size. It is possible that this will recover normally, "
                        "but may be you have more event listeners than connections available to the Redis pool. "
                        "You can increase the redis pull size by specifying the `maxsize` "
                        "parameter when instantiating each Redis transport. Current maxsize is: "
                        "".format(self.connection_kwargs.get('maxsize'))
                    )

            return await self._redis_pool
        except aioredis.PoolClosedError:
            raise LightbusShutdownInProgress('Redis connection pool has been closed. Assuming shutdown in progress.')


class RedisRpcTransport(RedisTransportMixin, RpcTransport):

    def __init__(self, redis_pool=None, *, track_consumption_progress=False,
                 serializer=ByFieldMessageSerializer(), deserializer=ByFieldMessageDeserializer(RpcMessage),
                 **connection_kwargs
                 ):
        self.set_redis_pool(redis_pool)
        self.set_connection_kwargs(connection_kwargs)
        self._latest_ids = {}
        self.track_consumption_progress = track_consumption_progress  # TODO: Implement (rename: replay?)
        self.serializer = serializer
        self.deserializer = deserializer

    async def call_rpc(self, rpc_message: RpcMessage, options: dict):
        stream = '{}:stream'.format(rpc_message.api_name)
        logger.debug(
            LBullets(
                L("Enqueuing message {} in Redis stream {}", Bold(rpc_message), Bold(stream)),
                items=dict(**rpc_message.get_metadata(), kwargs=rpc_message.get_kwargs())
            )
        )

        with await self.connection_manager() as redis:
            start_time = time.time()
            # TODO: MAXLEN
            await redis.xadd(stream=stream, fields=self.serializer(rpc_message))

        logger.info(L(
            "Enqueued message {} in Redis in {} stream {}",
            Bold(rpc_message), human_time(time.time() - start_time), Bold(stream)
        ))

    async def consume_rpcs(self, apis: Sequence[Api]) -> Sequence[RpcMessage]:
        # Get the name of each stream
        streams = ['{}:stream'.format(api.meta.name) for api in apis]
        # Get where we last left off in each stream
        latest_ids = [self._latest_ids.get(stream, '$') for stream in streams]

        logger.debug(LBullets(
            'Consuming RPCs from', items=[
                '{} ({})'.format(s, self._latest_ids.get(s, '$')) for s in streams
            ]
        ))

        with await self.connection_manager() as redis:
            # TODO: Count/timeout configurable
            try:
                stream_messages = await redis.xread(streams, latest_ids=latest_ids, count=10)
            except RuntimeError:
                # For some reason aio-redis likes to eat the CancelledError and
                # turn it into a Runtime error:
                # https://github.com/aio-libs/aioredis/blob/9f5964/aioredis/connection.py#L184
                raise asyncio.CancelledError('aio-redis task was cancelled and decided it should be a RuntimeError')

        rpc_messages = []
        for stream, message_id, fields in stream_messages:
            stream = decode(stream, 'utf8')
            message_id = decode(message_id, 'utf8')
            rpc_message = self.deserializer(fields)

            # See comment on events transport re updating message_id
            self._latest_ids[stream] = message_id
            rpc_messages.append(rpc_message)
            logger.debug(LBullets(
                L("⬅ Received message {} on stream {}", Bold(message_id), Bold(stream)),
                items=dict(**rpc_message.get_metadata(), kwargs=rpc_message.get_kwargs())
            ))

        return rpc_messages


class RedisResultTransport(RedisTransportMixin, ResultTransport):

    def __init__(self, redis_pool=None, *,
                 serializer=BlobMessageSerializer(), deserializer=BlobMessageDeserializer(ResultMessage),
                 **connection_kwargs
                 ):
        # NOTE: We use the blob serializer here, as the results come back as values in a list
        self.set_redis_pool(redis_pool)
        self.set_connection_kwargs(connection_kwargs)
        self.serializer = serializer
        self.deserializer = deserializer

    def get_return_path(self, rpc_message: RpcMessage) -> str:
        return 'redis+key://{}.{}:result:{}'.format(
            rpc_message.api_name,
            rpc_message.procedure_name,
            rpc_message.rpc_id,
        )

    async def send_result(self, rpc_message: RpcMessage, result_message: ResultMessage, return_path: str):
        logger.debug(L(
            "Sending result {} into Redis using return path {}",
            Bold(result_message), Bold(return_path)
        ))
        redis_key = self._parse_return_path(return_path)

        with await self.connection_manager() as redis:
            start_time = time.time()
            p = redis.pipeline()
            p.lpush(redis_key, self.serializer(result_message))
            # TODO: Make result expiry configurable
            p.expire(redis_key, timeout=60)
            await p.execute()

        logger.debug(L(
            "➡ Sent result {} into Redis in {} using return path {}",
            Bold(result_message), human_time(time.time() - start_time), Bold(return_path)
        ))

    async def receive_result(self, rpc_message: RpcMessage, return_path: str, options: dict) -> ResultMessage:
        logger.info(L("⌛ Awaiting Redis result for RPC message: {}", Bold(rpc_message)))
        redis_key = self._parse_return_path(return_path)

        with await self.connection_manager() as redis:
            start_time = time.time()
            result = None
            while not result:
                # Sometimes blpop() will return None in the case of timeout or
                # cancellation. We therefore perform this step with a loop to catch
                # this. A more elegant solution is welcome.
                # TODO: Make timeout configurable
                result = await redis.blpop(redis_key, timeout=5)
            _, serialized = result

        result_message = self.deserializer(serialized)

        logger.info(L(
            "⬅ Received Redis result in {} for RPC message {}: {}",
            human_time(time.time() - start_time), rpc_message, Bold(result_message.result)
        ))

        return result_message

    def _parse_return_path(self, return_path: str) -> str:
        assert return_path.startswith('redis+key://')
        return return_path[12:]


class RedisEventTransport(RedisTransportMixin, EventTransport):

    def __init__(self, redis_pool=None, *,
                 serializer=ByFieldMessageSerializer(), deserializer=ByFieldMessageDeserializer(EventMessage),
                 track_consumption_progress=False, **connection_kwargs
                 ):
        self.set_redis_pool(redis_pool)
        self.set_connection_kwargs(connection_kwargs)
        self.track_consumption_progress = track_consumption_progress  # TODO: Implement (rename: replay?)
        self.serializer = serializer
        self.deserializer = deserializer

        self._task = None
        self._reload = False

    async def send_event(self, event_message: EventMessage, options: dict):
        """Publish an event"""
        stream = '{}.{}:stream'.format(event_message.api_name, event_message.event_name)
        logger.debug(
            LBullets(
                L("Enqueuing event message {} in Redis stream {}", Bold(event_message), Bold(stream)),
                items=dict(**event_message.get_metadata(), kwargs=event_message.get_kwargs())
            )
        )

        with await self.connection_manager() as redis:
            start_time = time.time()
            # TODO: MAXLEN
            await redis.xadd(stream=stream, fields=self.serializer(event_message))

        logger.info(L(
            "Enqueued event message {} in Redis in {} stream {}",
            Bold(event_message), human_time(time.time() - start_time), Bold(stream)
        ))

    async def fetch(self, listen_for, context: dict, since: Union[Since, Sequence[Since]] = '$', forever=True
                    ) -> Generator[EventMessage, None, None]:

        if not isinstance(since, (list, tuple)):
            since = [since] * len(listen_for)
        since = map(normalise_since_value, since)

        # Keys are stream names, values as the latest ID consumed from that stream
        stream_names = ['{}.{}:stream'.format(api, name) for api, name in listen_for]

        # Setup our context to have sensible defaults
        context.setdefault('streams', OrderedDict())
        # We'll use the `streams` variable as shorthand for `context['streams']`
        streams = context['streams']

        for stream_name, stream_since in zip(stream_names, since):
            streams.setdefault(stream_name, stream_since)

        while True:
            # Fetch some messages
            with await self.connection_manager() as redis:
                logger.info(LBullets(
                    'Consuming events from', items={
                        '{} ({})'.format(*v) for v in streams.items()
                    }
                ))
                try:
                    # This will block until there are some messages available
                    stream_messages = await redis.xread(
                        streams=list(streams.keys()),
                        latest_ids=list(streams.values()),
                        count=10,  # TODO: Make configurable, add timeout too
                    )
                except aioredis.ConnectionForcedCloseError:
                    return

            # Handle the messages we have received
            for stream, message_id, fields in stream_messages:
                stream = decode(stream, 'utf8')
                message_id = decode(message_id, 'utf8')

                # Unfortunately, there is an edge-case when BOTH:
                #  1. We are consuming events from 'now' (i.e. event ID '$'), the default
                #  2. There is an unhandled error when processing the FIRST batch of events
                # In which case, the next iteration would start again from '$', in which
                # case we would loose events. Therefore 'subtract one' from the message ID
                # and store that immediately. Subtracting one is imprecise, as there is a SLIM
                # chance we could grab another event in the process. However, if events are
                # being consumed from 'now' then the developer presumably doesn't care about
                # a high level of precision.
                if streams[stream] == '$':
                    streams[stream] = redis_stream_id_subtract_one(message_id)

                event_message = self.deserializer(fields)

                logger.debug(LBullets(
                    L("⬅ Received event {} on stream {}", Bold(message_id), Bold(stream)),
                    items=dict(**event_message.get_metadata(), kwargs=event_message.get_kwargs())
                ))

                # TODO: Consider subclassing EventMessage as RedisEventMessage
                event_message.redis_id = message_id
                event_message.redis_stream = stream

                yield event_message

            if not forever:
                return

    async def consumption_complete(self, event_message: EventMessage, context: dict):
        context['streams'][event_message.redis_stream] = event_message.redis_id


class RedisSchemaTransport(RedisTransportMixin, SchemaTransport):

    def __init__(self, redis_pool=None, **connection_kwargs):
        self.set_redis_pool(redis_pool)
        self.set_connection_kwargs(connection_kwargs)
        self._latest_ids = {}

    def schema_key(self, api_name):
        return 'schema:{}'.format(api_name)

    def schema_set_key(self):
        """Maintains a set of api names in redis which can be used to retrieve individual schemas"""
        return 'schemas'

    async def store(self, api_name: str, schema: Dict, ttl_seconds: Optional[int]):
        """Store an individual schema"""
        with await self.connection_manager() as redis:
            schema_key = self.schema_key(api_name)

            p = redis.pipeline()
            p.set(schema_key, json.dumps(schema))
            if ttl_seconds is not None:
                p.expire(schema_key, ttl_seconds)
            p.sadd(self.schema_set_key(), api_name)
            await p.execute()

    async def load(self) -> Dict[str, Dict]:
        """Load all schemas"""
        schemas = {}
        with await self.connection_manager() as redis:
            # Get & decode the api names
            logger.debug('Current loop: {}'.format(id(asyncio.get_event_loop())))
            logger.debug('Redis loop: {}'.format(id(redis._pool_or_conn._loop)))
            api_names = list(await redis.smembers(self.schema_set_key()))
            api_names = [api_name.decode('utf8') for api_name in api_names]

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
    milliseconds, n = map(int, message_id.split('-'))
    if n > 0:
        n = n - 1
    elif milliseconds > 0:
        milliseconds = milliseconds - 1
        n = 9999
    else:
        # message_id is '0000000000000-0'. Subtracting one
        # from this is neither possible, desirable or useful.
        return message_id
    return '{:13d}-{}'.format(milliseconds, n)


def normalise_since_value(since):
    """Take a 'since' value and normalise it to be a redis message ID"""
    if not since:
        return '$'
    elif hasattr(since, 'timestamp'):  # datetime
        # Create message ID: "<milliseconds-timestamp>-<sequence-number>"
        return '{}-0'.format(round(since.timestamp() * 1000))
    else:
        return since


class InvalidRedisPool(LightbusException):
    pass
