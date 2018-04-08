import json
import logging
import time
from datetime import datetime
from typing import Sequence, Optional, Union, Generator, Dict, NamedTuple, Mapping
from urllib.parse import urlparse

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
from lightbus.schema.encoder import json_encode
from lightbus.serializers.blob import BlobMessageSerializer, BlobMessageDeserializer
from lightbus.serializers.by_field import ByFieldMessageSerializer, ByFieldMessageDeserializer
from lightbus.transports.base import ResultTransport, RpcTransport, EventTransport, SchemaTransport
from lightbus.utilities.frozendict import frozendict
from lightbus.utilities.human import human_time
from lightbus.utilities.importing import import_from_string

logger = logging.getLogger(__name__)

Since = Union[str, datetime, None]


class RedisTransportMixin(object):
    connection_parameters: dict = {
        'address': 'redis://localhost:6379',
        'maxsize': 100,
    }
    _redis_pool: Optional[Redis] = None

    def set_redis_pool(self, redis_pool: Optional[Redis], url: str=None, connection_parameters: Mapping=frozendict()):
        if not redis_pool:
            self.connection_parameters = self.connection_parameters.copy()
            self.connection_parameters.update(connection_parameters)
            if url:
                self.connection_parameters['address'] = url
        else:
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

    async def connection_manager(self) -> Redis:
        if self._redis_pool is None:
            self._redis_pool = await aioredis.create_redis_pool(**self.connection_parameters)

        try:
            internal_pool = self._redis_pool._pool_or_conn
            if hasattr(internal_pool, 'size') and hasattr(internal_pool, 'maxsize'):
                if internal_pool.size == internal_pool.maxsize:
                    logging.critical(
                        "Redis pool has reached maximum size. It is possible that this will recover normally, "
                        "but may be you have more event listeners than connections available to the Redis pool. "
                        "You can increase the redis pull size by specifying the `maxsize` "
                        "parameter when instantiating each Redis transport. Current maxsize is: "
                        "".format(self.connection_parameters.get('maxsize'))
                    )

            return await self._redis_pool
        except aioredis.PoolClosedError:
            raise LightbusShutdownInProgress('Redis connection pool has been closed. Assuming shutdown in progress.')

    async def close(self):
        if self._redis_pool:
            self._redis_pool.close()
            await self._redis_pool.wait_closed()
            self._redis_pool = None


class RedisRpcTransport(RedisTransportMixin, RpcTransport):
    """ Redis RPC transport providing at-most-once delivery

    This transport uses a redis list and a blocking pop operation
    to distribute an RPC call to a single RPC consumer.

    Each call also has a corresponding expiry key created. Once the
    key expires it should be assumed that the RPC call has timed
    out and that therefore is should be discarded rather than
    be processed.
    """

    def __init__(self, *,
                 redis_pool=None,
                 url=None,
                 serializer=BlobMessageSerializer(),
                 deserializer=BlobMessageDeserializer(RpcMessage),
                 connection_parameters: Mapping=frozendict(maxsize=100),
                 batch_size=10,
                 rpc_timeout=5,
                 ):
        self.set_redis_pool(redis_pool, url, connection_parameters)
        self._latest_ids = {}
        self.serializer = serializer
        self.deserializer = deserializer
        self.batch_size = batch_size
        self.rpc_timeout = rpc_timeout

    @classmethod
    def from_config(cls,
                    config,
                    url: str='redis://127.0.0.1:6379/0',
                    connection_parameters: Mapping=frozendict(maxsize=100),
                    batch_size: int=10,
                    serializer: str='lightbus.serializers.BlobMessageSerializer',
                    deserializer: str='lightbus.serializers.BlobMessageDeserializer',
                    rpc_timeout=5,
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
        )

    async def call_rpc(self, rpc_message: RpcMessage, options: dict):
        queue_key = f'{rpc_message.api_name}:rpc_queue'
        expiry_key = f'rpc_expiry_key:{rpc_message.rpc_id}'
        logger.debug(
            LBullets(
                L("Enqueuing message {} in Redis stream {}", Bold(rpc_message), Bold(queue_key)),
                items=dict(**rpc_message.get_metadata(), kwargs=rpc_message.get_kwargs())
            )
        )

        with await self.connection_manager() as redis:
            start_time = time.time()
            print('setting ' + expiry_key)
            p = redis.pipeline()
            p.rpush(key=queue_key, value=self.serializer(rpc_message))
            p.set(expiry_key, 1)
            p.expire(expiry_key, timeout=self.rpc_timeout)
            await p.execute()

        logger.debug(L(
            "Enqueued message {} in Redis in {} stream {}",
            Bold(rpc_message), human_time(time.time() - start_time), Bold(queue_key)
        ))

    async def consume_rpcs(self, apis: Sequence[Api]) -> Sequence[RpcMessage]:
        # Get the name of each stream
        queue_keys = ['{}:rpc_queue'.format(api.meta.name) for api in apis]

        logger.debug(LBullets(
            'Consuming RPCs from', items=[
                '{} ({})'.format(s, self._latest_ids.get(s, '$')) for s in queue_keys
            ]
        ))

        with await self.connection_manager() as redis:
            try:
                stream, data = await redis.blpop(*queue_keys)
            except RuntimeError:
                # For some reason aio-redis likes to eat the CancelledError and
                # turn it into a Runtime error:
                # https://github.com/aio-libs/aioredis/blob/9f5964/aioredis/connection.py#L184
                raise asyncio.CancelledError('aio-redis task was cancelled and decided it should be a RuntimeError')

            stream = decode(stream, 'utf8')
            rpc_message = self.deserializer(data)
            expiry_key = f'rpc_expiry_key:{rpc_message.rpc_id}'
            print('deleting ' + expiry_key)
            key_deleted = await redis.delete(expiry_key)

            if not key_deleted:
                return []

            logger.debug(LBullets(
                L("⬅ Received RPC message on stream {}", Bold(stream)),
                items=dict(**rpc_message.get_metadata(), kwargs=rpc_message.get_kwargs())
            ))

        return [rpc_message]


class RedisResultTransport(RedisTransportMixin, ResultTransport):

    def __init__(self, *,
                 redis_pool=None,
                 url=None,
                 serializer=BlobMessageSerializer(),
                 deserializer=BlobMessageDeserializer(ResultMessage),
                 connection_parameters: Mapping=frozendict(maxsize=100),
                 result_ttl=60,
                 rpc_timeout=5,
                 ):
        # NOTE: We use the blob serializer here, as the results come back as values in a list
        self.set_redis_pool(redis_pool, url, connection_parameters)
        self.serializer = serializer
        self.deserializer = deserializer
        self.result_ttl = result_ttl
        self.rpc_timeout = rpc_timeout

    @classmethod
    def from_config(cls,
                    config,
                    url: str='redis://127.0.0.1:6379/0',
                    serializer: str='lightbus.serializers.BlobMessageSerializer',
                    deserializer: str='lightbus.serializers.BlobMessageDeserializer',
                    connection_parameters: Mapping=frozendict(maxsize=100),
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
            p.expire(redis_key, timeout=self.result_ttl)
            await p.execute()

        logger.debug(L(
            "➡ Sent result {} into Redis in {} using return path {}",
            Bold(result_message), human_time(time.time() - start_time), Bold(return_path)
        ))

    async def receive_result(self, rpc_message: RpcMessage, return_path: str, options: dict) -> ResultMessage:
        logger.debug(L("Awaiting Redis result for RPC message: {}", Bold(rpc_message)))
        redis_key = self._parse_return_path(return_path)

        with await self.connection_manager() as redis:
            start_time = time.time()
            result = None
            while not result:
                # Sometimes blpop() will return None in the case of timeout or
                # cancellation. We therefore perform this step with a loop to catch
                # this. A more elegant solution is welcome.
                result = await redis.blpop(redis_key, timeout=self.rpc_timeout)
            _, serialized = result

        result_message = self.deserializer(serialized)

        logger.debug(L(
            "⬅ Received Redis result in {} for RPC message {}: {}",
            human_time(time.time() - start_time), rpc_message, Bold(result_message.result)
        ))

        return result_message

    def _parse_return_path(self, return_path: str) -> str:
        assert return_path.startswith('redis+key://')
        return return_path[12:]


class RedisEventTransport(RedisTransportMixin, EventTransport):
    # TODO: Use a consumer group to make sure we don't miss events while the process is
    # offline. For example, sending welcome email upon user registration. This means
    # we need to collect an app name somewhere

    def __init__(self, redis_pool=None, *,
                 url=None,
                 serializer=ByFieldMessageSerializer(),
                 deserializer=ByFieldMessageDeserializer(EventMessage),
                 connection_parameters: Mapping=frozendict(maxsize=100),
                 batch_size=10,
                 ):
        self.set_redis_pool(redis_pool, url, connection_parameters)
        self.serializer = serializer
        self.deserializer = deserializer
        self.batch_size = batch_size

        self._task = None
        self._reload = False

    @classmethod
    def from_config(cls,
                    config,
                    url: str='redis://127.0.0.1:6379/0',
                    connection_parameters: Mapping=frozendict(maxsize=100),
                    batch_size: int=10,
                    serializer: str='lightbus.serializers.ByFieldMessageSerializer',
                    deserializer: str='lightbus.serializers.ByFieldMessageDeserializer',
                    ):
        serializer = import_from_string(serializer)()
        deserializer = import_from_string(deserializer)(EventMessage)

        return cls(
            redis_pool=None,
            url=url,
            connection_parameters=connection_parameters,
            batch_size=batch_size,
            serializer=serializer,
            deserializer=deserializer,
        )

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

        logger.debug(L(
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
                logger.debug(LBullets(
                    'Consuming events from', items={
                        '{} ({})'.format(*v) for v in streams.items()
                    }
                ))
                try:
                    # This will block until there are some messages available
                    stream_messages = await redis.xread(
                        streams=list(streams.keys()),
                        latest_ids=list(streams.values()),
                        count=self.batch_size,
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

                event_message.redis_id = message_id
                event_message.redis_stream = stream

                yield event_message

            if not forever:
                return

    async def consumption_complete(self, event_message: EventMessage, context: dict):
        context['streams'][event_message.redis_stream] = event_message.redis_id


class RedisSchemaTransport(RedisTransportMixin, SchemaTransport):

    def __init__(self, *,
                 redis_pool=None,
                 url: str = 'redis://127.0.0.1:6379/0',
                 connection_parameters: Mapping=frozendict()
                 ):
        self.set_redis_pool(redis_pool, url, connection_parameters)
        self._latest_ids = {}

    @classmethod
    def from_config(cls,
                    config,
                    url: str='redis://127.0.0.1:6379/0',
                    connection_parameters: Mapping=frozendict(),
                    ):
        return cls(
            url=url,
            connection_parameters=connection_parameters,
        )

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
