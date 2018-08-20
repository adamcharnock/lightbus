import asyncio

import pytest

from lightbus import TransactionalEventTransport, DebugEventTransport
from lightbus.path import BusPath
from lightbus.exceptions import (
    NoApisSpecified,
    ApisMustUseTransactionalTransport,
    ApisMustUseSameTransport,
)
from lightbus.transports.transactional import lightbus_set_database

pytestmark = pytest.mark.unit


@pytest.fixture()
def atomic_context(dummy_bus: BusPath, aiopg_connection):
    registry = dummy_bus.client.transport_registry
    current_event_transport = registry.get_event_transport("default")
    registry.set_event_transport(
        "default", TransactionalEventTransport(child_transport=current_event_transport)
    )
    return lightbus_set_database(dummy_bus, aiopg_connection, apis=["some_api", "some_api2"])


def test_init_no_apis(dummy_bus: BusPath, aiopg_connection):
    with pytest.raises(NoApisSpecified):
        lightbus_set_database(dummy_bus, aiopg_connection, apis=[])


def test_init_bad_transport(dummy_bus: BusPath, aiopg_connection):
    with pytest.raises(ApisMustUseTransactionalTransport):
        lightbus_set_database(dummy_bus, aiopg_connection, apis=["some_api"])


def test_init_multiple_transports(dummy_bus: BusPath, aiopg_connection):
    registry = dummy_bus.client.transport_registry
    registry.set_event_transport("another_api", TransactionalEventTransport(DebugEventTransport()))

    with pytest.raises(ApisMustUseSameTransport):
        lightbus_set_database(dummy_bus, aiopg_connection, apis=["some_api", "another_api"])


@pytest.mark.asyncio
async def test_aenter(atomic_context, aiopg_connection):
    await atomic_context.__aenter__()
    assert atomic_context.cursor
    assert atomic_context.transport.connection == aiopg_connection
    assert atomic_context.transport.cursor


@pytest.mark.asyncio
async def test_aexit_success(atomic_context, mocker):
    f = asyncio.Future()
    f.set_result(None)
    rollback_mock = mocker.patch.object(
        atomic_context.transport, "rollback_and_finish", return_value=f
    )
    commit_mock = mocker.patch.object(atomic_context.transport, "commit_and_finish", return_value=f)

    await atomic_context.__aenter__()
    cursor = atomic_context.cursor

    await atomic_context.__aexit__(None, None, None)
    assert not rollback_mock.called
    assert commit_mock.called
    assert not atomic_context.cursor
    assert not cursor.closed


@pytest.mark.asyncio
async def test_aexit_exception(atomic_context, mocker):
    f = asyncio.Future()
    f.set_result(None)
    rollback_mock = mocker.patch.object(
        atomic_context.transport, "rollback_and_finish", return_value=f
    )
    commit_mock = mocker.patch.object(atomic_context.transport, "commit_and_finish", return_value=f)

    await atomic_context.__aenter__()
    cursor = atomic_context.cursor

    await atomic_context.__aexit__(True, True, True)
    assert rollback_mock.called
    assert not commit_mock.called
    assert not atomic_context.cursor
    assert not cursor.closed
