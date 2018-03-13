import pytest

from lightbus.config import Config
from lightbus.config.config import mapping_to_named_tuple
from lightbus.config.structure import RootConfig, BusConfig

pytestmark = pytest.mark.unit


def test_config_schema():
    schema = Config(
        root_config=RootConfig(bus=BusConfig(), apis=[])
    ).schema()

    assert '$schema' in schema
    assert 'properties' in schema
    assert schema['additionalProperties'] == False
    assert schema['type'] == 'object'


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
    assert config.api('foo').event_transport.parameters.batch_size == 50


def test_api_config_customised():
    config = Config.load_yaml(EXAMPLE_VALID_YAML)
    assert config.api('my.api').event_transport.parameters.batch_size == 1


EXAMPLE_VALID_YAML = """
bus:
    log_level: warning

apis:
    default:
        event_transport:
            transport: lightbus.RedisRpcTransport
            parameters:
                batch_size: 50
                
    my.api:
        event_transport:
            parameters:
                batch_size: 1
"""
