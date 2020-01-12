import json
import logging
from typing import Mapping, Dict, Optional

from lightbus.transports.base import SchemaTransport
from lightbus.schema.encoder import json_encode
from lightbus.transports.redis.utilities import RedisTransportMixin
from lightbus.utilities.frozendict import frozendict

logger = logging.getLogger("lightbus.transports.redis")


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
        super().__init__()

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
