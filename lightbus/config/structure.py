from typing import NamedTuple, Optional


class BusConfig(NamedTuple):
    schema_load_timeout: int = 5
    schema_add_api_timeout: int = 1
    schema_human_readable: bool = True
    log_level: str = 'debug'


class ApiConfig(NamedTuple):
    rpc_timeout: int = 5
    event_listener_setup_timeout: int = 1
    event_fire_timeout: int = 1
    # Implement per-api log_level using logging context manager
    log_level: Optional[str] = None


class TransportConfig(NamedTuple):
    pass


class RedisTransportConfig(TransportConfig):
    url: str = 'redis://127.0.0.1:6379/0'
    pool_parameters: dict = dict(maxsize=100)
    serializer: str
    deserializer: str


class RedisEventTransportConfig(TransportConfig):
    batch_size: int = 10
    serializer: str = 'lightbus.serializers.ByFieldMessageSerializer'
    # NOTE: This will need to be passed the class it needs to deserialize into
    deserializer: str = 'lightbus.serializers.ByFieldMessageDeserializer'


class RedisResultTransportConfig(TransportConfig):
    result_ttl: int = 60
    serializer: str = 'lightbus.serializers.BlobMessageSerializer'
    # NOTE: This will need to be passed the class it needs to deserialize into
    deserializer: str = 'lightbus.serializers.BlobMessageDeserializer'


class RedisRpcTransportConfig(TransportConfig):
    batch_size: int = 10
    serializer: str = 'lightbus.serializers.ByFieldMessageSerializer'
    # NOTE: This will need to be passed the class it needs to deserialize into
    deserializer: str = 'lightbus.serializers.ByFieldMessageDeserializer'


class StatePluginConfig(NamedTuple):
    ping_enabled: bool = True
    ping_interval: int = 60
