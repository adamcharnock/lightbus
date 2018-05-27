import pytest

from lightbus import BusNode, TransactionalEventTransport, DebugEventTransport
from lightbus.exceptions import (
    NoApisSpecified,
    ApisMustUseTransactionalTransport,
    ApisMustUseSameTransport,
)
from lightbus.transports.transactional import lightbus_atomic

pytestmark = pytest.mark.unit


def test_init_ok(dummy_bus: BusNode, aiopg_connection):
    registry = dummy_bus.bus_client.transport_registry
    current_event_transport = registry.get_event_transport("default")
    registry.set_event_transport(
        "default", TransactionalEventTransport(child_transport=current_event_transport)
    )

    lightbus_atomic(dummy_bus, aiopg_connection, apis=["some_api", "some_api2"])


def test_init_no_apis(dummy_bus: BusNode, aiopg_connection):
    with pytest.raises(NoApisSpecified):
        lightbus_atomic(dummy_bus, aiopg_connection, apis=[])


def test_init_bad_transport(dummy_bus: BusNode, aiopg_connection):
    with pytest.raises(ApisMustUseTransactionalTransport):
        lightbus_atomic(dummy_bus, aiopg_connection, apis=["some_api"])


def test_init_multiple_transports(dummy_bus: BusNode, aiopg_connection):
    registry = dummy_bus.bus_client.transport_registry
    registry.set_event_transport("another_api", TransactionalEventTransport(DebugEventTransport()))

    with pytest.raises(ApisMustUseSameTransport):
        lightbus_atomic(dummy_bus, aiopg_connection, apis=["some_api", "another_api"])
