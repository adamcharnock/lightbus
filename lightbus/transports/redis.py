import asyncio
import json
import logging

import asyncio_extras
from collections import OrderedDict
from typing import Sequence, Tuple, Optional
from uuid import uuid1

import time
from aioredis.util import decode
from aioredis.pool import ConnectionsPool
import aioredis
from aioredis import Redis

from lightbus.exceptions import InvalidRedisPool
from lightbus.transports.base import ResultTransport, RpcTransport, EventTransport
from lightbus.api import Api
from lightbus.log import L, Bold, LBullets
from lightbus.message import RpcMessage, ResultMessage, EventMessage
from lightbus.utilities import human_time

logger = logging.getLogger(__name__)

# TODO: There is a lot of duplicated code here, particularly between the RPC transport & event transport


class RedisTransportMixin(object):
    connection_kwargs: {}
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

    async def get_redis_pool(self) -> Redis:
        if self._redis_pool is None:
            self._redis_pool = await aioredis.create_redis_pool(**self.connection_kwargs)
        return self._redis_pool


class RedisRpcTransport(RedisTransportMixin, RpcTransport):

    def __init__(self, redis_pool=None, *, track_consumption_progress=False, **connection_kwargs):
        self.set_redis_pool(redis_pool)
        self.connection_kwargs = connection_kwargs or dict(address=('localhost', 6379))
        self._latest_ids = {}
        self.track_consumption_progress = track_consumption_progress  # TODO: Implement (rename: replay?)

    async def call_rpc(self, rpc_message: RpcMessage):
        stream = '{}:stream'.format(rpc_message.api_name)
        logger.debug(
            LBullets(
                L("Enqueuing message {} in Redis stream {}", Bold(rpc_message), Bold(stream)),
                items=rpc_message.to_dict()
            )
        )

        pool = await self.get_redis_pool()
        with await pool as redis:
            start_time = time.time()
            # TODO: MAXLEN
            await redis.xadd(stream=stream, fields=rpc_message.to_dict())

        logger.info(L(
            "Enqueued message {} in Redis in {} stream {}",
            Bold(rpc_message), human_time(time.time() - start_time), Bold(stream)
        ))

    async def consume_rpcs(self, apis: Sequence[Api]) -> Sequence[RpcMessage]:
        # Get the name of each stream
        streams = ['{}:stream'.format(api.meta.name) for api in apis]
        # Get where we last left off in each stream
        latest_ids = [self._latest_ids.get(stream, '$') for stream in streams]

        pool = await self.get_redis_pool()
        with await pool as redis:
            # TODO: Count/timeout configurable
            stream_messages = await redis.xread(streams, latest_ids=latest_ids, count=10)

        rpc_messages = []
        for stream, message_id, fields in stream_messages:
            stream = decode(stream, 'utf8')
            message_id = decode(message_id, 'utf8')
            decoded_fields = decode_message_fields(fields)

            # See comment on events transport re updating message_id
            self._latest_ids[stream] = message_id
            rpc_messages.append(
                RpcMessage.from_dict(decoded_fields)
            )
            logger.debug(LBullets(
                L("⬅ Received message {} on stream {}", Bold(message_id), Bold(stream)),
                items=decoded_fields
            ))

        return rpc_messages


class RedisResultTransport(RedisTransportMixin, ResultTransport):

    def __init__(self, redis_pool=None, **connection_kwargs):
        self.set_redis_pool(redis_pool)
        self.connection_kwargs = connection_kwargs or dict(address=('localhost', 6379))

    def get_return_path(self, rpc_message: RpcMessage) -> str:
        return 'redis+key://{}.{}:result:{}'.format(rpc_message.api_name, rpc_message.procedure_name, uuid1().hex)

    async def send_result(self, rpc_message: RpcMessage, result_message: ResultMessage, return_path: str):
        logger.debug(L(
            "Sending result {} into Redis using return path {}",
            Bold(result_message), Bold(return_path)
        ))
        redis_key = self._parse_return_path(return_path)

        pool = await self.get_redis_pool()
        with await pool as redis:
            start_time = time.time()
            p = redis.pipeline()
            p.lpush(redis_key, redis_encode(result_message.result))
            # TODO: Make result expiry configurable
            p.expire(redis_key, timeout=60)
            await p.execute()

        logger.debug(L(
            "➡ Sent result {} into Redis in {} using return path {}",
            Bold(result_message), human_time(time.time() - start_time), Bold(return_path)
        ))

    async def receive_result(self, rpc_message: RpcMessage, return_path: str) -> ResultMessage:
        logger.info(L("⌛ Awaiting Redis result for RPC message: {}", Bold(rpc_message)))
        redis_key = self._parse_return_path(return_path)

        pool = await self.get_redis_pool()
        with await pool as redis:
            start_time = time.time()
            # TODO: Make timeout configurable
            _, result = await redis.blpop(redis_key, timeout=5)
            result = redis_decode(result)

        logger.info(L(
            "⬅ Received Redis result in {} for RPC message {}: {}",
            human_time(time.time() - start_time), rpc_message, Bold(result)
        ))
        return result

    def _parse_return_path(self, return_path: str) -> str:
        assert return_path.startswith('redis+key://')
        return return_path[12:]


class RedisEventTransport(RedisTransportMixin, EventTransport):

    def __init__(self, redis_pool=None, *, track_consumption_progress=False, **connection_kwargs):
        self.set_redis_pool(redis_pool)
        self.connection_kwargs = connection_kwargs or dict(address=('localhost', 6379))
        self.track_consumption_progress = track_consumption_progress  # TODO: Implement (rename: replay?)

        self._task = None
        self._reload = False
        self._streams = OrderedDict()

    async def send_event(self, event_message: EventMessage):
        """Publish an event"""
        stream = '{}.{}:stream'.format(event_message.api_name, event_message.event_name)
        logger.debug(
            LBullets(
                L("Enqueuing event message {} in Redis stream {}", Bold(event_message), Bold(stream)),
                items=event_message.to_dict()
            )
        )

        pool = await self.get_redis_pool()
        with await pool as redis:
            start_time = time.time()
            # TODO: MAXLEN
            await redis.xadd(stream=stream, fields=event_message.to_dict())

        logger.info(L(
            "Enqueued event message {} in Redis in {} stream {}",
            Bold(event_message), human_time(time.time() - start_time), Bold(stream)
        ))

    async def fetch_events(self) -> Tuple[Sequence[EventMessage], ...]:
        pool = await self.get_redis_pool()
        with await pool as redis:
            if not self._streams:
                logger.debug('Event backend has been given no events to consume. Sleeping.')
                self._task = asyncio.ensure_future(asyncio.sleep(3600 * 24 * 365))
            else:
                logger.info(LBullets(
                    'Consuming events from', items={
                        '{} ({})'.format(*v) for v in self._streams.items()
                    }
                ))
                # TODO: Count/timeout
                self._task = asyncio.ensure_future(
                    redis.xread(
                        streams=list(self._streams.keys()),
                        latest_ids=list(self._streams.values()),
                        count=10,  # TODO: Make configurable, add timeout too
                    )
                )

            try:
                stream_messages = await self._task or []
            except asyncio.CancelledError as e:
                if self._reload:
                    # Streams to listen on have changed.
                    # Bail out and let consume_events() get called again,
                    # at which point we'll pickup the new streams.
                    logger.debug('Event consumption cancelled.')
                    stream_messages = []
                    self._reload = False
                else:
                    raise

        event_messages = []
        latest_ids = {}
        for stream, message_id, fields in stream_messages:
            stream = decode(stream, 'utf8')
            message_id = decode(message_id, 'utf8')
            decoded_fields = decode_message_fields(fields)

            # Keep track of which event ID we are up to. We will store these
            # in consumption_complete(), once we know the events have definitely
            # been consumed.
            latest_ids[stream] = message_id

            # Unfortunately, these is an edge-case when BOTH:
            #  1. We are consuming events from 'now' (i.e. event ID '$'), the default
            #  2. There is an unhandled error when processing the FIRST batch of events
            # In which case, the next iteration would start again from '$', in which
            # case we would loose events. Therefore 'subtract one' from the message ID
            # and store that immediately. Subtracting one is imprecise, as there is a SLIM
            # chance we could grab another event in the process. However, if events are
            # being consumed from 'now' then the developer presumably doesn't care about
            # a high level of precision.
            if self._streams[stream] == '$':
                self._streams[stream] = redis_stream_id_subtract_one(message_id)

            event_messages.append(
                EventMessage.from_dict(decoded_fields)
            )
            logger.debug(LBullets(
                L("⬅ Received event {} on stream {}", Bold(message_id), Bold(stream)),
                items=decoded_fields
            ))

        return event_messages, latest_ids

    async def consumption_complete(self, latest_ids):
        self._streams.update(latest_ids)

    async def start_listening_for(self, api_name, event_name):
        stream_name = '{}.{}:stream'.format(api_name, event_name)
        if stream_name in self._streams:
            logger.debug('Already listening on event stream {}. Doing nothing.'.format(stream_name))
        else:
            logger.info(L(
                'Will to listen on event stream {} {}',
                Bold(stream_name),
                'starting now' if self._task else 'once event consumption begins',
            ))
            self._streams[stream_name] = '$'

            if self._task:
                logger.debug('Existing consumer task running, cancelling')
                self._reload = True
                self._task.cancel()

    async def stop_listening_for(self, api_name, event_name):
        raise NotImplementedError()


def redis_encode(value):
    # TODO: Some kind of encoding/schema/types here. This all just needs some serious thought
    return json.dumps(value)


def redis_decode(data):
    # TODO: Some kind of encoding/schema/types here. This all just needs some serious thought
    return json.loads(data)


def decode_message_fields(fields):
    return OrderedDict([
        (
            decode(k, encoding='utf8'),
            decode(v, encoding='utf8'),
        )
        for k, v
        in fields.items()
    ])


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
