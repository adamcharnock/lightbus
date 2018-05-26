import pytest

from lightbus import (
    RedisRpcTransport,
    RedisResultTransport,
    RedisEventTransport,
    RedisSchemaTransport,
    DebugRpcTransport,
    DebugEventTransport,
)
from lightbus.config import Config
from lightbus.exceptions import TransportNotFound
from lightbus.transports.base import TransportRegistry

pytestmark = pytest.mark.unit


@pytest.fixture()
def redis_default_config():
    return Config.load_dict(
        {
            "bus": {"schema": {"transport": {"redis": {}}}},
            "apis": {
                "default": {
                    "rpc_transport": {"redis": {}},
                    "result_transport": {"redis": {}},
                    "event_transport": {"redis": {}},
                }
            },
        }
    )


@pytest.fixture()
def redis_other_config():
    return Config.load_dict(
        {
            "apis": {
                "other": {
                    "rpc_transport": {"redis": {}},
                    "result_transport": {"redis": {}},
                    "event_transport": {"redis": {}},
                }
            }
        }
    )


def test_transport_registry_get_does_not_exist_default():
    registry = TransportRegistry()
    with pytest.raises(TransportNotFound):
        assert not registry.get_rpc_transport("default")
    with pytest.raises(TransportNotFound):
        assert not registry.get_result_transport("default")
    with pytest.raises(TransportNotFound):
        assert not registry.get_event_transport("default")
    with pytest.raises(TransportNotFound):
        assert not registry.get_schema_transport()


def test_transport_registry_get_does_not_exist_default_default_value():
    registry = TransportRegistry()
    assert registry.get_rpc_transport("default", default=None) is None
    assert registry.get_result_transport("default", default=None) is None
    assert registry.get_event_transport("default", default=None) is None
    assert registry.get_schema_transport(default=None) is None


def test_transport_registry_get_does_not_exist_other():
    registry = TransportRegistry()
    with pytest.raises(TransportNotFound):
        assert not registry.get_rpc_transport("other")
    with pytest.raises(TransportNotFound):
        assert not registry.get_result_transport("other")
    with pytest.raises(TransportNotFound):
        assert not registry.get_event_transport("other")
    with pytest.raises(TransportNotFound):
        assert not registry.get_schema_transport()


def test_transport_registry_get_fallback(redis_default_config):
    registry = TransportRegistry().load_config(redis_default_config)
    assert registry.get_rpc_transport("default").__class__ == RedisRpcTransport
    assert registry.get_result_transport("default").__class__ == RedisResultTransport
    assert registry.get_event_transport("default").__class__ == RedisEventTransport
    assert registry.get_schema_transport("default").__class__ == RedisSchemaTransport


def test_transport_registry_get_specific_api(redis_other_config):
    registry = TransportRegistry().load_config(redis_other_config)
    assert registry.get_rpc_transport("other").__class__ == RedisRpcTransport
    assert registry.get_result_transport("other").__class__ == RedisResultTransport
    assert registry.get_event_transport("other").__class__ == RedisEventTransport
    assert registry.get_schema_transport("other").__class__ == RedisSchemaTransport


def test_transport_registry_load_config(redis_default_config):
    registry = TransportRegistry().load_config(redis_default_config)
    assert registry.get_rpc_transport("default").__class__ == RedisRpcTransport
    assert registry.get_result_transport("default").__class__ == RedisResultTransport
    assert registry.get_event_transport("default").__class__ == RedisEventTransport
    assert registry.get_schema_transport("default").__class__ == RedisSchemaTransport


def test_transport_registry_get_rpc_transports(redis_default_config):
    registry = TransportRegistry().load_config(redis_default_config)
    debug_transport = DebugRpcTransport()
    redis_transport = RedisRpcTransport()

    registry.set_rpc_transport("redis1", redis_transport)
    registry.set_rpc_transport("redis2", redis_transport)
    registry.set_rpc_transport("debug1", debug_transport)
    registry.set_rpc_transport("debug2", debug_transport)
    transports = registry.get_rpc_transports(
        ["default", "foo", "bar", "redis1", "redis2", "debug1", "debug2"]
    )

    default_redis_transport = registry.get_rpc_transport("default")

    transports = dict(transports)
    assert set(transports[default_redis_transport]) == {"default", "foo", "bar"}
    assert set(transports[debug_transport]) == {"debug1", "debug2"}
    assert set(transports[redis_transport]) == {"redis1", "redis2"}


def test_transport_registry_get_event_transports(redis_default_config):
    registry = TransportRegistry().load_config(redis_default_config)
    debug_transport = DebugEventTransport()
    redis_transport = RedisEventTransport(consumer_group_prefix="foo", consumer_name="bar")

    registry.set_event_transport("redis1", redis_transport)
    registry.set_event_transport("redis2", redis_transport)
    registry.set_event_transport("debug1", debug_transport)
    registry.set_event_transport("debug2", debug_transport)
    transports = registry.get_event_transports(
        ["default", "foo", "bar", "redis1", "redis2", "debug1", "debug2"]
    )

    default_redis_transport = registry.get_event_transport("default")

    transports = dict(transports)
    assert set(transports[default_redis_transport]) == {"default", "foo", "bar"}
    assert set(transports[debug_transport]) == {"debug1", "debug2"}
    assert set(transports[redis_transport]) == {"redis1", "redis2"}


def test_get_all_transports(redis_default_config):
    registry = TransportRegistry().load_config(redis_default_config)
    registry.set_event_transport("another", registry.get_event_transport("default"))
    registry.set_event_transport("foo", DebugEventTransport())
    assert len(registry.get_all_transports()) == 4
