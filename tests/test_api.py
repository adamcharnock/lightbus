import pytest

from lightbus import Api, Event
from lightbus.api import Registry
from lightbus.exceptions import (
    MisconfiguredApiOptions,
    InvalidApiEventConfiguration,
    InvalidApiRegistryEntry,
    UnknownApi,
)

pytestmark = pytest.mark.unit


@pytest.fixture()
def SimpleApi():
    class SimpleApi(Api):
        class Meta:
            name = "simple.api"

    return SimpleApi


@pytest.fixture()
def registry():
    return Registry()


def test_api_named_default():
    # Apis can not start with the name 'default'
    with pytest.raises(MisconfiguredApiOptions):

        class BadApi(Api):
            class Meta:
                name = "default"


def test_api_named_default_dot_something():
    # Apis can not start with the name 'default'
    with pytest.raises(MisconfiguredApiOptions):

        class BadApi(Api):
            class Meta:
                name = "default.foo"


def test_pass_string_as_event_params():
    # Check we cannot accidentally pass a string to Event in the
    # case that we omit a ',' when specifying a parameters tuple
    with pytest.raises(InvalidApiEventConfiguration):
        Event(parameters=("foo"))


def test_api_registry_add_ok(SimpleApi, registry):
    registry.add(SimpleApi())
    assert "simple.api" in registry._apis


def test_api_registry_add_class(SimpleApi, registry):
    with pytest.raises(InvalidApiRegistryEntry):
        registry.add(SimpleApi)


def test_api_registry_get_ok(SimpleApi, registry):
    api = SimpleApi()
    registry.add(api)
    assert registry.get("simple.api") == api


def test_api_registry_get_unknown(SimpleApi, registry):
    with pytest.raises(UnknownApi):
        registry.get("unknown.api")


def test_api_registry_remove_ok(SimpleApi, registry):
    registry.add(SimpleApi())
    registry.remove("simple.api")
    assert not registry._apis


def test_api_registry_remove_unknown(SimpleApi, registry):
    with pytest.raises(UnknownApi):
        registry.remove("unknown.api")


def test_api_registry_internal(registry):
    class InternalApi(Api):
        class Meta:
            name = "internal.api"
            internal = True

    api = InternalApi()
    registry.add(api)
    assert registry.internal() == [api]
    assert registry.public() == []


def test_api_registry_public(SimpleApi, registry):
    api = SimpleApi()
    registry.add(api)
    assert registry.public() == [api]
    assert registry.internal() == []


def test_api_registry_all(SimpleApi, registry):
    api = SimpleApi()
    registry.add(api)
    assert registry.all() == [api]


def test_api_registry_names(SimpleApi, registry):
    api = SimpleApi()
    registry.add(api)
    assert registry.names() == ["simple.api"]
