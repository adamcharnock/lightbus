import asyncio
import logging
from typing import Sequence

import aioredis

from lightbus.transports.base import ResultTransport, RpcTransport
from lightbus.api import Api
from lightbus.log import L, Bold
from lightbus.message import RpcMessage, ResultMessage, EventMessage


logger = logging.getLogger(__name__)


class RedisRpcTransport(RpcTransport):

    def __init__(self, host='localhost', port=6379, db=None, password=None, ssl=None,
                 track_consumption_progress=False, redis=None,
                 **connection_extra):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.ssl = ssl
        self.connection_extra = connection_extra
        self._redis = redis
        self._latest_ids = {}
        self.track_consumption_progress = track_consumption_progress  # TODO: Implement (rename: replay?)

    async def get_redis(self) -> 'aioredis.Redis':
        if self._redis is None:
            # TODO: Will using only a single connection cause things to lockup if we use redis twice simultaneously?
            #       Should this ever happen anyway? If so, use a pool.
            self._redis = await aioredis.create_redis(
                host=self.host, port=self.port, db=self.db, password=self.password, ssl=self.ssl,
                **self.connection_extra
            )
        return self._redis

    async def call_rpc(self, rpc_message: RpcMessage):
        # Direct RPC transport calls API method immediately
        stream = '{}_stream'.format(rpc_message.api_name)
        logger.debug(L("Enqueuing message {} in Redis stream {}", Bold(rpc_message), Bold(rpc_message)))

        redis = await self.get_redis()
        # TODO: MAXLEN
        await redis.xadd(stream=stream, fields=rpc_message.to_dict())

        logger.info(L("Enqueued message {} in Redis stream {}", Bold(rpc_message), Bold(rpc_message)))

    async def consume_rpcs(self, apis: Sequence[Api]) -> Sequence[RpcMessage]:
        # I would very much like to use an asynchronous generator here,
        # but that would mean ditching support for Python 3.5 which may
        # be somewhat jumping the gun.

        # Get the name of each stream
        streams = ['{}_stream'.format(api.meta.name) for api in apis]
        # Get where we last left off in each stream
        latest_ids = [self._latest_ids.get(stream, '$') for stream in streams]

        logger.debug("Consuming RPCs from {} stream(s)".format(len(streams)))

        redis = await self.get_redis()
        # TODO: Count/timeout
        messages = await redis.xread(streams, latest_ids=latest_ids)

        for stream, message_id, _ in messages:
            self._latest_ids[stream] = message_id

        return [RpcMessage.from_dict(fields) for _, _, fields in messages]

    async def send_event(self, api, name, kwargs):
        """Publish an event"""
        pass

    async def consume_events(self, api) -> EventMessage:
        """Consume RPC events for the given API"""
        pass


class RedisResultTransport(ResultTransport):

    def get_return_path(self, rpc_message: RpcMessage) -> str:
        logger.debug("Attaching future to RPC message")
        rpc_message._direct_result_transport_future = asyncio.Future()
        return ''

    async def send(self, rpc_message: RpcMessage, result_message: ResultMessage):
        logger.info(L("⚡️  Directly sending RPC result: {}", Bold(result_message)))
        rpc_message._direct_result_transport_future.set_result(result_message)

    async def receive(self, rpc_message: RpcMessage) -> ResultMessage:
        logger.info(L("⌛️  Awaiting result for RPC message: {}", Bold(rpc_message)))
        result = await rpc_message._direct_result_transport_future
        logger.info(L("✅  Received result for RPC message {}: {}", rpc_message, Bold(result)))
        return result
