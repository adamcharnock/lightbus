import asyncio
import json
import logging
from collections import OrderedDict
from typing import Sequence, Tuple
from uuid import uuid1

import aioredis
import time
from aioredis.util import decode

from lightbus.transports.base import ResultTransport, RpcTransport
from lightbus.api import Api
from lightbus.log import L, Bold, LBullets
from lightbus.message import RpcMessage, ResultMessage, EventMessage
from lightbus.utilities import human_time

logger = logging.getLogger(__name__)


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
        # Direct RPC transport calls API method immediately
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
        # I would very much like to use an asynchronous generator here,
        # but that would mean ditching support for Python 3.5 which may
        # be somewhat jumping the gun.

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
            decoded_fields = self._decode_fields(fields)

            self._latest_ids[stream] = message_id
            rpc_messages.append(
                RpcMessage.from_dict(decoded_fields)
            )
            logger.debug(LBullets(
                L("Received message {} on stream {}", Bold(message_id), Bold(stream)),
                items=decoded_fields
            ))

        return rpc_messages

    def _decode_fields(self, fields):
        return OrderedDict([
            (
                decode(k, encoding='utf8'),
                v if k.startswith(b'kw:') else decode(v, encoding='utf8'),
            )
            for k, v
            in fields.items()
        ])

    async def send_event(self, api, name, kwargs):
        """Publish an event"""
        pass

    async def consume_events(self, api) -> EventMessage:
        """Consume RPC events for the given API"""
        pass


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

    async def send(self, rpc_message: RpcMessage, result_message: ResultMessage):
        logger.debug(L(
            "Sending result {} into Redis using return path {}",
            Bold(result_message), Bold(rpc_message.return_path)
        ))
        redis_key = self._parse_return_path(rpc_message.return_path)
        redis = await self.get_redis()

        # TODO: Make result expiry configurable
        start_time = time.time()
        p = redis.pipeline()
        p.lpush(redis_key, redis_encode(result_message.result))
        p.expire(redis_key, timeout=60)
        await p.execute()

        logger.debug(L(
            "Sent result {} into Redis in {} using return path {}",
            Bold(result_message), human_time(time.time() - start_time), Bold(rpc_message.return_path)
        ))

    async def receive(self, rpc_message: RpcMessage) -> ResultMessage:
        logger.info(L("⌛️  Awaiting Redis result for RPC message: {}", Bold(rpc_message)))
        redis = await self.get_redis()
        redis_key = self._parse_return_path(rpc_message.return_path)
        # TODO: Make timeout configurable

        start_time = time.time()
        _, result = await redis.blpop(redis_key, timeout=5)
        result = redis_decode(result)

        logger.info(L(
            "✅  Received Redis result in {} for RPC message {}: {}",
            human_time(time.time() - start_time), rpc_message, Bold(result)
        ))
        return result

    def _parse_return_path(self, return_path: str) -> str:
        assert return_path.startswith('redis+key://')
        return return_path[12:]


def redis_encode(value):
    # TODO: Some kind of encoding/schema/types here. This all just needs some serious thought
    return json.dumps(value)


def redis_decode(data):
    # TODO: Some kind of encoding/schema/types here. This all just needs some serious thought
    return json.loads(data)

