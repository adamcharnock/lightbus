import asyncio
import logging
import time
from typing import Mapping, Sequence, TYPE_CHECKING

from lightbus_vendored.aioredis import PipelineError, ConnectionClosedError, ReplyError
from lightbus_vendored.aioredis.util import decode

from lightbus.transports.base import RpcTransport, RpcMessage, Api
from lightbus.exceptions import TransportIsClosed
from lightbus.log import LBullets, L, Bold
from lightbus.serializers import BlobMessageSerializer, BlobMessageDeserializer
from lightbus.transports.redis.utilities import (
    RedisTransportMixin,
    retry_on_redis_connection_failure,
)
from lightbus.utilities.frozendict import frozendict
from lightbus.utilities.human import human_time
from lightbus.utilities.importing import import_from_string

if TYPE_CHECKING:
    # pylint: disable=unused-import,cyclic-import
    from lightbus.config import Config
    from lightbus.client import BusClient

logger = logging.getLogger("lightbus.transports.redis")


class RedisRpcTransport(RedisTransportMixin, RpcTransport):
    """Redis RPC transport providing at-most-once delivery

    This transport uses a redis list and a blocking pop operation
    to distribute an RPC call to a single RPC consumer.

    Each call also has a corresponding expiry key created. Once the
    key expires it should be assumed that the RPC call has timed
    out and that therefore should be discarded rather than
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
        rpc_retry_delay=1,
        consumption_restart_delay=5,
    ):
        self.set_redis_pool(redis_pool, url, connection_parameters)
        self._latest_ids = {}
        self.serializer = serializer
        self.deserializer = deserializer
        self.batch_size = batch_size
        self.rpc_timeout = rpc_timeout
        self.rpc_retry_delay = rpc_retry_delay
        self.consumption_restart_delay = consumption_restart_delay
        super().__init__()

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
        rpc_retry_delay: int = 1,
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
        """Emit a call to a remote procedure

        This only sends the request, it does not await any result (see RedisResultTransport)
        """
        queue_key = f"{rpc_message.api_name}:rpc_queue"
        expiry_key = f"rpc_expiry_key:{rpc_message.id}"
        logger.debug(
            LBullets(
                L("Enqueuing message {} in Redis list {}", Bold(rpc_message), Bold(queue_key)),
                items=dict(**rpc_message.get_metadata(), kwargs=rpc_message.get_kwargs()),
            )
        )

        start_time = time.time()
        for try_number in range(3, 0, -1):
            last_try = try_number == 1
            try:
                await self._call_rpc(rpc_message, queue_key, expiry_key)
                break
            except (PipelineError, ConnectionClosedError, ConnectionResetError) as e:
                if not last_try:
                    logger.debug(
                        f"Retrying sending of message. Will retry {try_number} more times. "
                        f"Error was {type(e).__name__}: {e}"
                    )
                    await asyncio.sleep(self.rpc_retry_delay)
                else:
                    raise

        logger.debug(
            L(
                "Enqueued message {} in Redis in {} stream {}",
                Bold(rpc_message),
                human_time(time.time() - start_time),
                Bold(queue_key),
            )
        )

    async def _call_rpc(self, rpc_message: RpcMessage, queue_key, expiry_key):
        with await self.connection_manager() as redis:
            p = redis.pipeline()
            p.rpush(key=queue_key, value=self.serializer(rpc_message))
            p.set(expiry_key, 1)
            # TODO: Ditch this somehow. We kind of need the ApiConfig here, but a transports can be
            #       used for multiple APIs. So we could pass it in for each use of the transports,
            #       but even then it doesn't work for the event listening. This is because an event listener
            #       can span multiple APIs
            p.expire(expiry_key, timeout=self.rpc_timeout)
            await p.execute()

    async def consume_rpcs(self, apis: Sequence[Api]) -> Sequence[RpcMessage]:
        """Consume RPCs for the given APIs"""
        if self._closed:
            # Triggered during shutdown
            raise TransportIsClosed("Transport is closed. Cannot consume RPCs")

        return await retry_on_redis_connection_failure(
            fn=self._consume_rpcs,
            args=[apis],
            retry_delay=self.consumption_restart_delay,
            action="consuming RPCs",
        )

    async def _consume_rpcs(self, apis: Sequence[Api]) -> Sequence[RpcMessage]:
        # Get the name of each list queue
        queue_keys = ["{}:rpc_queue".format(api.meta.name) for api in apis]

        logger.debug(
            LBullets(
                "Consuming RPCs from",
                items=["{} ({})".format(s, self._latest_ids.get(s, "$")) for s in queue_keys],
            )
        )

        with await self.connection_manager() as redis:
            try:
                try:
                    stream, data = await redis.blpop(*queue_keys)
                except RuntimeError:
                    # For some reason aio-redis likes to eat the CancelledError and
                    # turn it into a Runtime error:
                    # https://github.com/aio-libs/aioredis/blob/9f5964/aioredis/connection.py#L184
                    raise asyncio.CancelledError(
                        "aio-redis task was cancelled and decided it should be a RuntimeError"
                    )
            except asyncio.CancelledError:
                # We need to manually close the connection here otherwise the aioredis
                # pool will emit warnings saying that this connection still has pending
                # commands (i.e. the above blocking pop)
                logger.debug("Closing redis connection")
                redis.close()
                raise

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
