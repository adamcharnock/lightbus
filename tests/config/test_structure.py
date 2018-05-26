import pytest

from lightbus.config.structure import make_transport_selector_structure, ApiConfig, RootConfig

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
