import pytest

from lightbus import DebugRpcTransport
from lightbus.config import Config
from lightbus.config.config import mapping_to_named_tuple, config_as_json_schema
from lightbus.config.structure import RootConfig, BusConfig
from lightbus.transports.redis import RedisRpcTransport

pytestmark = pytest.mark.unit


def test_config_as_json_schema():
    schema = config_as_json_schema()

    assert '$schema' in schema
    assert 'bus' in schema['properties']
    assert schema['additionalProperties'] == False
    assert schema['type'] == 'object'


def test_config_as_json_schema_bus():
    schema = config_as_json_schema()
    assert schema['properties']['bus']


def test_config_as_json_schema_apis():
    schema = config_as_json_schema()
    assert schema['properties']['apis']


def test_default_config():
    config = Config.load_dict({})
    assert config.bus()
    assert config.api()
    assert config.api().rpc_transport.redis is None
    assert config.api().result_transport.redis is None
    assert config.api().event_transport.redis is None
    assert config.api().schema_transport.redis is None


def test_load_bus_config(tmp_file):
    tmp_file.write('bus: { log_level: warning }')
    tmp_file.flush()
    config = Config.load_file(tmp_file.name)
    assert config.bus().log_level == 'warning'


def test_mapping_to_named_tuple_ok():
    root_config = mapping_to_named_tuple({'bus': {'log_level': 'warning'}}, RootConfig)
    assert root_config.bus.log_level == 'warning'


def test_mapping_to_named_tuple_apis():
    root_config = mapping_to_named_tuple({'apis': {'my_api': {'rpc_timeout': 1}}}, RootConfig)
    assert root_config.apis['my_api'].rpc_timeout == 1


def test_mapping_to_named_tuple_unknown_property():
    root_config = mapping_to_named_tuple({'bus': {'foo': 'xyz'}}, RootConfig)
    assert not hasattr(root_config.bus, 'foo')


def test_api_config_default():
    config = Config.load_yaml(EXAMPLE_VALID_YAML)
    assert config.api('foo').event_transport.redis.batch_size == 50


def test_api_config_customised():
    config = Config.load_yaml(EXAMPLE_VALID_YAML)
    assert config.api('my.api').event_transport.redis.batch_size == 1


EXAMPLE_VALID_YAML = """
bus:
    log_level: warning

apis:
    default:
        event_transport:
            redis:
                batch_size: 50
                
    my.api:
        event_transport:
            redis:
                batch_size: 1
"""
