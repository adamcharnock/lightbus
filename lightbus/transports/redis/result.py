import logging
import time
from typing import Mapping

from lightbus.transports.base import ResultTransport, ResultMessage, RpcMessage
from lightbus.log import L, Bold
from lightbus.serializers import BlobMessageSerializer, BlobMessageDeserializer
from lightbus.transports.redis.utilities import RedisTransportMixin
from lightbus.utilities.frozendict import frozendict
from lightbus.utilities.human import human_time
from lightbus.utilities.importing import import_from_string

if False:
    # pylint: disable=unused-import
    from lightbus.config import Config
    from lightbus.client import BusClient

logger = logging.getLogger("lightbus.transports.redis")


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
        self,
        rpc_message: RpcMessage,
        result_message: ResultMessage,
        return_path: str,
        bus_client: "BusClient",
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
        self, rpc_message: RpcMessage, return_path: str, options: dict, bus_client: "BusClient"
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
