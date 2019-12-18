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
from lightbus.transports.registry import TransportRegistry, get_transport, get_transport_name

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


@pytest.fixture()
def redis_no_default_config():
    return Config.load_dict(
        {
            "bus": {"schema": {"transport": {"redis": {}}}},
            "apis": {
                "other": {
                    "rpc_transport": {"redis": {}},
                    "result_transport": {"redis": {}},
                    "event_transport": {"redis": {}},
                }
            },
        },
        set_defaults=False,
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
    assert registry.get_rpc_transport("default").transport_class == RedisRpcTransport
    assert registry.get_result_transport("default").transport_class == RedisResultTransport
    assert registry.get_event_transport("default").transport_class == RedisEventTransport
    assert registry.get_schema_transport("default").transport_class == RedisSchemaTransport


def test_transport_registry_get_specific_api(redis_other_config):
    registry = TransportRegistry().load_config(redis_other_config)
    assert registry.get_rpc_transport("other").transport_class == RedisRpcTransport
    assert registry.get_result_transport("other").transport_class == RedisResultTransport
    assert registry.get_event_transport("other").transport_class == RedisEventTransport
    assert registry.get_schema_transport("other").transport_class == RedisSchemaTransport


def test_transport_registry_load_config(redis_default_config):
    registry = TransportRegistry().load_config(redis_default_config)
    assert registry.get_rpc_transport("default").transport_class == RedisRpcTransport
    assert registry.get_result_transport("default").transport_class == RedisResultTransport
    assert registry.get_event_transport("default").transport_class == RedisEventTransport
    assert registry.get_schema_transport("default").transport_class == RedisSchemaTransport


def test_transport_registry_get_rpc_transports(redis_default_config):
    registry = TransportRegistry().load_config(redis_default_config)

    # Note how we set a config value below. We do this because
    # we need this transport to appear different from the default
    # transport for the purposes of this test. Also note that these
    # are not actual transports, but transport pools which wrap transports.
    # We are therefore actually comparing transport pools. Transport
    # pools are considered equal if they have the same transport class
    # and config. It is for this reason we modify the config below.
    registry.set_rpc_transport(
        "redis1", RedisRpcTransport, RedisRpcTransport.Config(rpc_timeout=99), Config.default()
    )
    registry.set_rpc_transport(
        "redis2", RedisRpcTransport, RedisRpcTransport.Config(rpc_timeout=99), Config.default()
    )
    registry.set_rpc_transport(
        "debug1", DebugRpcTransport, DebugRpcTransport.Config(), Config.default()
    )
    registry.set_rpc_transport(
        "debug2", DebugRpcTransport, DebugRpcTransport.Config(), Config.default()
    )

    transport_pools = registry.get_rpc_transports(
        ["default", "foo", "bar", "redis1", "redis2", "debug1", "debug2"]
    )

    default_transport = registry.get_rpc_transport("default")
    redis_transport = registry.get_rpc_transport("redis1")
    debug_transport = registry.get_rpc_transport("debug1")

    assert set(transport_pools[default_transport]) == {"default", "foo", "bar"}
    assert set(transport_pools[debug_transport]) == {"debug1", "debug2"}
    assert set(transport_pools[redis_transport]) == {"redis1", "redis2"}


def test_transport_registry_get_event_transports(redis_default_config):
    registry = TransportRegistry().load_config(redis_default_config)

    # See comment in test_transport_registry_get_rpc_transports
    registry.set_event_transport(
        "redis1", RedisEventTransport, RedisEventTransport.Config(batch_size=99), Config.default()
    )
    registry.set_event_transport(
        "redis2", RedisEventTransport, RedisEventTransport.Config(batch_size=99), Config.default()
    )
    registry.set_event_transport(
        "debug1", DebugEventTransport, DebugEventTransport.Config(), Config.default()
    )
    registry.set_event_transport(
        "debug2", DebugEventTransport, DebugEventTransport.Config(), Config.default()
    )

    transports = registry.get_event_transports(
        ["default", "foo", "bar", "redis1", "redis2", "debug1", "debug2"]
    )

    default_transport = registry.get_event_transport("default")
    redis_transport = registry.get_event_transport("redis1")
    debug_transport = registry.get_event_transport("debug1")

    assert set(transports[default_transport]) == {"default", "foo", "bar"}
    assert set(transports[debug_transport]) == {"debug1", "debug2"}
    assert set(transports[redis_transport]) == {"redis1", "redis2"}


def test_get_all_transports(redis_default_config):
    registry = TransportRegistry().load_config(redis_default_config)
    registry.set_event_transport("another", registry.get_event_transport("default"))
    registry.set_event_transport("foo", DebugEventTransport())
    assert len(registry.get_all_transports()) == 4


def test_has_rpc_transport(redis_no_default_config):
    registry = TransportRegistry().load_config(redis_no_default_config)
    assert registry.has_rpc_transport("other")
    assert not registry.has_rpc_transport("foo")


def test_has_result_transport(redis_no_default_config):
    registry = TransportRegistry().load_config(redis_no_default_config)
    assert registry.has_result_transport("other")
    assert not registry.has_result_transport("foo")


def test_has_event_transport(redis_no_default_config):
    registry = TransportRegistry().load_config(redis_no_default_config)
    assert registry.has_event_transport("other")
    assert not registry.has_event_transport("foo")


def test_has_schema_transport(redis_no_default_config):
    registry = TransportRegistry().load_config(redis_no_default_config)
    assert registry.has_schema_transport()
    registry.schema_transport = None
    assert not registry.has_schema_transport()


def test_get_transport():
    assert get_transport("rpc", "redis")
    with pytest.raises(TransportNotFound):
        get_transport("rpc", "foo")


def test_get_transport_name():
    assert get_transport_name(RedisEventTransport) == "redis"

    class FakeTransport:
        pass

    with pytest.raises(TransportNotFound):
        get_transport_name(FakeTransport)
