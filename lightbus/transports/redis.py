import asyncio
import json
import logging
from collections import OrderedDict
from typing import Sequence, Tuple
from uuid import uuid1

import aioredis
import time
from aioredis.util import decode

from lightbus.transports.base import ResultTransport, RpcTransport, EventTransport
from lightbus.api import Api
from lightbus.log import L, Bold, LBullets
from lightbus.message import RpcMessage, ResultMessage, EventMessage
from lightbus.utilities import human_time

logger = logging.getLogger(__name__)

# TODO: There is a lot of duplicated code here, particularly between the RPC transport & event transport


class RedisRpcTransport(RpcTransport):

    def __init__(self, redis=None, *, track_consumption_progress=False, **connection_kwargs):
        self._redis = redis
        self.connection_kwargs = connection_kwargs or dict(address=('localhost', 6379))
        self._latest_ids = {}
        self.track_consumption_progress = track_consumption_progress  # TODO: Implement (rename: replay?)

    async def get_redis(self) -> 'aioredis.Redis':
        if self._redis is None:
            self._redis = await aioredis.create_redis(**self.connection_kwargs)
        return self._redis

    async def call_rpc(self, rpc_message: RpcMessage):
        stream = '{}:stream'.format(rpc_message.api_name)
        logger.debug(
            LBullets(
                L("Enqueuing message {} in Redis stream {}", Bold(rpc_message), Bold(stream)),
                items=rpc_message.to_dict()
            )
        )

        redis = await self.get_redis()
        # TODO: MAXLEN
        start_time = time.time()
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

        redis = await self.get_redis()
        # TODO: Count/timeout
        stream_messages = await redis.xread(streams, latest_ids=latest_ids)

        rpc_messages = []
        for stream, message_id, fields in stream_messages:
            stream = decode(stream, 'utf8')
            message_id = decode(message_id, 'utf8')
            decoded_fields = decode_message_fields(fields)

            self._latest_ids[stream] = message_id
            rpc_messages.append(
                RpcMessage.from_dict(decoded_fields)
            )
            logger.debug(LBullets(
                L("⬅ Received message {} on stream {}", Bold(message_id), Bold(stream)),
                items=decoded_fields
            ))

        return rpc_messages


class RedisResultTransport(ResultTransport):

    def __init__(self, redis=None, **connection_kwargs):
        self._redis = redis
        self.connection_kwargs = connection_kwargs or dict(address=('localhost', 6379))

    async def get_redis(self) -> 'aioredis.Redis':
        if self._redis is None:
            self._redis = await aioredis.create_redis(**self.connection_kwargs)
        return self._redis

    def get_return_path(self, rpc_message: RpcMessage) -> str:
        return 'redis+key://{}.{}:result:{}'.format(rpc_message.api_name, rpc_message.procedure_name, uuid1().hex)

    async def send_result(self, rpc_message: RpcMessage, result_message: ResultMessage, return_path: str):
        logger.debug(L(
            "Sending result {} into Redis using return path {}",
            Bold(result_message), Bold(return_path)
        ))
        redis_key = self._parse_return_path(return_path)
        redis = await self.get_redis()

        # TODO: Make result expiry configurable
        start_time = time.time()
        p = redis.pipeline()
        p.lpush(redis_key, redis_encode(result_message.result))
        p.expire(redis_key, timeout=60)
        await p.execute()

        logger.debug(L(
            "➡ Sent result {} into Redis in {} using return path {}",
            Bold(result_message), human_time(time.time() - start_time), Bold(return_path)
        ))

    async def receive_result(self, rpc_message: RpcMessage, return_path: str) -> ResultMessage:
        logger.info(L("⌛ Awaiting Redis result for RPC message: {}", Bold(rpc_message)))
        redis = await self.get_redis()
        redis_key = self._parse_return_path(return_path)
        # TODO: Make timeout configurable

        start_time = time.time()
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


class RedisEventTransport(EventTransport):

    def __init__(self, redis=None, *, track_consumption_progress=False, **connection_kwargs):
        self._redis = redis
        self.connection_kwargs = connection_kwargs or dict(address=('localhost', 6379))
        self.track_consumption_progress = track_consumption_progress  # TODO: Implement (rename: replay?)

        self._task = None
        self._streams = OrderedDict()
        self._redis = None

    async def get_redis(self) -> 'aioredis.Redis':
        if self._redis is None:
            self._redis = await aioredis.create_redis(**self.connection_kwargs)
        return self._redis

    async def send_event(self, event_message: EventMessage):
        """Publish an event"""
        stream = '{}.{}:stream'.format(event_message.api_name, event_message.event_name)
        logger.debug(
            LBullets(
                L("Enqueuing event message {} in Redis stream {}", Bold(event_message), Bold(stream)),
                items=event_message.to_dict()
            )
        )

        redis = await self.get_redis()
        # TODO: MAXLEN
        start_time = time.time()
        await redis.xadd(stream=stream, fields=event_message.to_dict())

        logger.info(L(
            "Enqueued event message {} in Redis in {} stream {}",
            Bold(event_message), human_time(time.time() - start_time), Bold(stream)
        ))

    async def consume_events(self) -> Sequence[EventMessage]:
        redis = await self.get_redis()

        # TODO: Count/timeout
        if not self._streams:
            logger.debug('Event backend has been given no events to consume. Sleeping.')
            self._task = asyncio.ensure_future(asyncio.sleep(3600 * 24 * 365))
        else:
            logger.info(LBullets('Consuming events from', items=self._streams.keys()))
            self._task = asyncio.ensure_future(
                redis.xread(list(self._streams.keys()), latest_ids=list(self._streams.values()))
            )

        try:
            stream_messages = await self._task or []
        except asyncio.CancelledError as e:
            logger.debug('Event consumption cancelled.')
            stream_messages = []

        event_messages = []
        for stream, message_id, fields in stream_messages:
            stream = decode(stream, 'utf8')
            message_id = decode(message_id, 'utf8')
            decoded_fields = decode_message_fields(fields)

            self._streams[stream] = message_id
            event_messages.append(
                EventMessage.from_dict(decoded_fields)
            )
            logger.debug(LBullets(
                L("⬅ Received event {} on stream {}", Bold(message_id), Bold(stream)),
                items=decoded_fields
            ))

        return event_messages

    async def start_listening_for(self, api_name, event_name):
        stream_name = '{}.{}:stream'.format(api_name, event_name)
        if stream_name in self._streams:
            logger.debug('Already listening on event stream {}. Doing nothing.'.format(stream_name))
        else:
            logger.info('Beginning to listen on event stream {}'.format(stream_name))
            self._streams[stream_name] = '$'

            if self._task:
                logger.debug('Existing consumer task running, cancelling')
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
            v if k.startswith(b'kw:') else decode(v, encoding='utf8'),
        )
        for k, v
        in fields.items()
    ])
