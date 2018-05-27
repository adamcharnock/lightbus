import asyncio

import pytest

from lightbus import BusNode, TransactionalEventTransport, DebugEventTransport
from lightbus.exceptions import (
    NoApisSpecified,
    ApisMustUseTransactionalTransport,
    ApisMustUseSameTransport,
)
from lightbus.transports.transactional import lightbus_atomic

pytestmark = pytest.mark.unit


@pytest.fixture()
def atomic_context(dummy_bus: BusNode, aiopg_connection):
    registry = dummy_bus.bus_client.transport_registry
    current_event_transport = registry.get_event_transport("default")
    registry.set_event_transport(
        "default", TransactionalEventTransport(child_transport=current_event_transport)
    )
    return lightbus_atomic(dummy_bus, aiopg_connection, apis=["some_api", "some_api2"])


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


def test_autodetect_start_transaction_psycopg2_with_autocommit(atomic_context, psycopg2_connection):
    psycopg2_connection.autocommit = True
    assert atomic_context._autodetect_start_transaction(psycopg2_connection) == False


def test_autodetect_start_transaction_psycopg2_without_autocommit(
    atomic_context, psycopg2_connection
):
    psycopg2_connection.autocommit = False
    assert atomic_context._autodetect_start_transaction(psycopg2_connection) == True


def test_autodetect_start_transaction_aiopg(atomic_context, aiopg_connection):
    # aiopg must always have autocommit on in order for the underlying
    # psycopg2 library for function in asynchronous mode
    assert atomic_context._autodetect_start_transaction(aiopg_connection) == False


@pytest.mark.run_loop
async def test_aenter(atomic_context, aiopg_connection):
    await atomic_context.__aenter__()
    assert atomic_context.cursor
    assert atomic_context.transport.connection == aiopg_connection
    assert atomic_context.transport.cursor


@pytest.mark.run_loop
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


@pytest.mark.run_loop
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
