import pytest

from lightbus.config.structure import (
    make_transport_selector_structure,
    ApiConfig,
    RootConfig,
    ConfigProxy,
    ApiValidationConfig,
)

pytestmark = pytest.mark.unit


def test_make_transport_config_structure():
    EventTransportSelector = make_transport_selector_structure("event")
    assert "redis" in EventTransportSelector.__annotations__


def test_make_api_config_structure():
    assert "event_transport" in ApiConfig.__annotations__
    assert "rpc_transport" in ApiConfig.__annotations__
    assert "result_transport" in ApiConfig.__annotations__
    assert "validate" in ApiConfig.__annotations__


def test_root_config_service_name():
    service_name = RootConfig().service_name
    assert service_name
    assert type(service_name) == str
    assert len(service_name) > 3
    # No format parameters in there, should have been formatted upon instantiation
    assert "{" not in service_name


def test_root_config_process_name():
    process_name = RootConfig().process_name
    assert process_name
    assert type(process_name) == str
    assert len(process_name) > 3
    # No format parameters in there, should have been formatted upon instantiation
    assert "{" not in process_name


def test_config_proxy_top_level_primary():
    config = ApiConfig(rpc_timeout=100)
    config_fallback = ApiConfig(rpc_timeout=123)
    proxied_config = ConfigProxy(
        (config, {"rpc_timeout": 100}), (config_fallback, {"rpc_timeout": 123})
    )
    assert proxied_config.rpc_timeout == 100


def test_config_proxy_returns_casted_value():
    config = ApiConfig(rpc_timeout=100)
    config_fallback = ApiConfig(rpc_timeout=123)
    proxied_config = ConfigProxy(
        (config, {"rpc_timeout": "100"}), (config_fallback, {"rpc_timeout": "123"})
    )
    assert proxied_config.rpc_timeout == 100  # Integer not string


def test_config_proxy_top_level_fallback():
    config = ApiConfig()
    config_fallback = ApiConfig(rpc_timeout=123)
    proxied_config = ConfigProxy((config, {}), (config_fallback, {"rpc_timeout": 123}))
    assert proxied_config.rpc_timeout == 123


def test_config_proxy_top_default():
    config = ApiConfig()
    config_fallback = ApiConfig()
    proxied_config = ConfigProxy((config, {}), (config_fallback, {}))
    assert proxied_config.rpc_timeout == ApiConfig.rpc_timeout


def test_config_proxy_child_structure_direct():
    config = ApiConfig()
    validate_fallback = ApiValidationConfig(outgoing=False)
    config_fallback = ApiConfig(validate=validate_fallback)
    proxied_config = ConfigProxy((config, {}), (config_fallback, {"validate": {"outgoing": False}}))
    assert proxied_config.validate.outgoing == False


def test_config_proxy_child_structure_partial():
    """A partially existing branch, but fallback required to get the ultimate value"""
    validate = ApiValidationConfig(incoming=False)
    config = ApiConfig(validate=validate)

    validate_fallback = ApiValidationConfig(outgoing=False)
    config_fallback = ApiConfig(validate=validate_fallback)
    proxied_config = ConfigProxy(
        # Primary config provides 'incoming' setting
        (config, {"validate": {"incoming": False}}),
        # ... and fallback config provides 'outgoing'
        (config_fallback, {"validate": {"outgoing": False}}),
    )
    # Ask for outgoing. Should be false even though default is True
    assert proxied_config.validate.outgoing == False


def test_config_proxy_attribute_error():
    with pytest.raises(AttributeError) as e:
        # Raises an attribute error when trying to get the default value
        _ = ConfigProxy((ApiConfig(), {})).validate.foo

    with pytest.raises(AttributeError) as e:
        # Raises an attribute error when getting an attribute from a child
        _ = ConfigProxy((ApiConfig(), {"validate": {"outgoing": True}})).validate.foo
