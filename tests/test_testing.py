import pytest

from lightbus import EventMessage, RpcMessage
from lightbus.client.commands import SendEventCommand, CallRpcCommand
from lightbus.utilities import testing
from lightbus.utilities.testing import BusQueueMockerContext

pytestmark = pytest.mark.unit


@pytest.yield_fixture
def mock_result(dummy_bus):
    with BusQueueMockerContext(dummy_bus) as mocker_context:
        yield testing.MockResult(mocker_context, mock_responses={}, mock_events=set())


def test_foo(mock_result: testing.MockResult):
    pass


@pytest.mark.parametrize(
    "method_name", ["assert_events_fired", "assertEventFired"], ids=["snake", "camel"]
)
def test_mock_result_assert_events_fired_simple(c, method_name):
    assert_events_fired = getattr(mock_result, method_name)
    mock_result.mocker_context.event.to_transport.put_items = [
        (
            SendEventCommand(message=EventMessage(api_name="api", event_name="event"), options={}),
            None,
        )
    ]

    # No exception
    try:
        assert_events_fired("api.event")
    except AssertionError as e:
        assert False, f"{method_name} incorrectly raised an assertion error: {e}"

    with pytest.raises(AssertionError):
        assert_events_fired("api.bad_event")


@pytest.mark.parametrize(
    "method_name", ["assert_events_fired", "assertEventFired"], ids=["snake", "camel"]
)
def test_mock_result_assert_events_fired_times(mock_result: testing.MockResult, method_name):
    assert_events_fired = getattr(mock_result, method_name)
    mock_result.mocker_context.event.to_transport.put_items = [
        (
            SendEventCommand(message=EventMessage(api_name="api", event_name="event"), options={}),
            None,
        ),
        (
            SendEventCommand(message=EventMessage(api_name="api", event_name="event"), options={}),
            None,
        ),
    ]

    # No error
    try:
        assert_events_fired("api.event")
    except AssertionError as e:
        assert False, f"{method_name} incorrectly raised an assertion error: {e}"

    # No error
    try:
        assert_events_fired("api.event", times=2)
    except AssertionError as e:
        assert False, f"{method_name} incorrectly raised an assertion error: {e}"

    # Error
    with pytest.raises(AssertionError):
        assert_events_fired("api.event", times=0)

    # Error
    with pytest.raises(AssertionError):
        assert_events_fired("api.event", times=1)

    # Error
    with pytest.raises(AssertionError):
        assert_events_fired("api.event", times=3)


@pytest.mark.parametrize(
    "method_name", ["assert_events_fired", "assertEventFired"], ids=["snake", "camel"]
)
def test_mock_result_assert_events_fired_zero(mock_result: testing.MockResult, method_name):
    assert_events_fired = getattr(mock_result, method_name)
    mock_result.mocker_context.event.to_transport.put_items = []

    # No error
    try:
        assert_events_fired("api.event", times=0)
    except AssertionError as e:
        assert False, f"{method_name} incorrectly raised an assertion error: {e}"

    # Error
    with pytest.raises(AssertionError):
        assert_events_fired("api.event", times=1)


@pytest.mark.parametrize(
    "method_name", ["assert_event_not_fired", "assertEventNotFired"], ids=["snake", "camel"]
)
def test_mock_result_assert_event_not_fired(mock_result: testing.MockResult, method_name):
    assert_event_not_fired = getattr(mock_result, method_name)
    mock_result.mocker_context.event.to_transport.put_items = [
        (
            SendEventCommand(message=EventMessage(api_name="api", event_name="event"), options={}),
            None,
        )
    ]

    # No exception
    try:
        assert_event_not_fired("api.bad_event")
    except AssertionError as e:
        assert False, f"{method_name} incorrectly raised an assertion error: {e}"

    with pytest.raises(AssertionError):
        assert_event_not_fired("api.event")


@pytest.mark.parametrize(
    "method_name", ["get_event_messages", "getEventMessages"], ids=["snake", "camel"]
)
def test_mock_result_get_event_messages(mock_result: testing.MockResult, method_name):
    get_event_messages = getattr(mock_result, method_name)

    event1 = EventMessage(api_name="api", event_name="event1")
    event2 = EventMessage(api_name="api", event_name="event2")
    # fmt: off
    mock_result.mocker_context.event.to_transport.put_items = [
        (
            SendEventCommand(message=event1, options={}),
            None,
        ), (
            SendEventCommand(message=event2, options={}),
            None,
        ),
    ]
    # fmt: on

    assert get_event_messages() == [event1, event2]


@pytest.mark.parametrize(
    "method_name", ["get_event_messages", "getEventMessages"], ids=["snake", "camel"]
)
def test_mock_result_get_event_messages_filtered(mock_result: testing.MockResult, method_name):
    get_event_messages = getattr(mock_result, method_name)

    event1 = EventMessage(api_name="api", event_name="event1")
    event2 = EventMessage(api_name="api", event_name="event2")
    # fmt: off
    mock_result.mocker_context.event.to_transport.put_items = [
        (
            SendEventCommand(message=event1, options={}),
            None,
        ), (
            SendEventCommand(message=event2, options={}),
            None,
        ),
    ]
    # fmt: on

    assert get_event_messages("api.event2") == [event2]


@pytest.mark.parametrize(
    "method_name", ["assert_rpc_called", "assertRpcCalled"], ids=["snake", "camel"]
)
def test_mock_result_assert_rpc_called_simple(mock_result: testing.MockResult, method_name):
    assert_rpc_called = getattr(mock_result, method_name)

    rpc_message = RpcMessage(api_name="api", procedure_name="rpc")
    # fmt: off
    mock_result.mocker_context.rpc_result.to_transport.put_items = [
        (
            CallRpcCommand(message=rpc_message, options={}),
            None,
        ),
    ]
    # fmt: on

    # No exception
    try:
        assert_rpc_called("api.rpc")
    except AssertionError as e:
        assert False, f"{method_name} incorrectly raised an assertion error: {e}"

    with pytest.raises(AssertionError):
        assert_rpc_called("api.bad_rpc")


@pytest.mark.parametrize(
    "method_name", ["assert_rpc_called", "assertRpcCalled"], ids=["snake", "camel"]
)
def test_mock_result_assert_rpc_called_times(mock_result: testing.MockResult, method_name):
    assert_rpc_called = getattr(mock_result, method_name)

    rpc_message1 = RpcMessage(api_name="api", procedure_name="rpc")
    rpc_message2 = RpcMessage(api_name="api", procedure_name="rpc")
    # fmt: off
    mock_result.mocker_context.rpc_result.to_transport.put_items = [
        (
            CallRpcCommand(message=rpc_message1, options={}),
            None,
        ), (
            CallRpcCommand(message=rpc_message2, options={}),
            None,
        ),
    ]
    # fmt: on

    # No error
    try:
        assert_rpc_called("api.rpc")
    except AssertionError as e:
        assert False, f"{method_name} incorrectly raised an assertion error: {e}"

    # No error
    try:
        assert_rpc_called("api.rpc", times=2)
    except AssertionError as e:
        assert False, f"{method_name} incorrectly raised an assertion error: {e}"

    # Error
    with pytest.raises(AssertionError):
        assert_rpc_called("api.rpc", times=0)

    # Error
    with pytest.raises(AssertionError):
        assert_rpc_called("api.rpc", times=1)

    # Error
    with pytest.raises(AssertionError):
        assert_rpc_called("api.rpc", times=3)


@pytest.mark.parametrize(
    "method_name", ["assert_rpc_called", "assertRpcCalled"], ids=["snake", "camel"]
)
def test_mock_result_assert_rpc_called_zero(mock_result: testing.MockResult, method_name):
    assert_rpc_called = getattr(mock_result, method_name)

    mock_result.mocker_context.rpc_result.to_transport.put_items = []

    # No error
    try:
        assert_rpc_called("api.rpc", times=0)
    except AssertionError as e:
        assert False, f"{method_name} incorrectly raised an assertion error: {e}"

    # Error
    with pytest.raises(AssertionError):
        assert_rpc_called("api.rpc", times=1)


@pytest.mark.parametrize(
    "method_name", ["assert_rpc_not_called", "assertRpcNotCalled"], ids=["snake", "camel"]
)
def test_mock_result_assert_rpc_not_called(mock_result: testing.MockResult, method_name):
    assert_rpc_not_called = getattr(mock_result, method_name)

    rpc_message = RpcMessage(api_name="api", procedure_name="rpc")
    # fmt: off
    mock_result.mocker_context.rpc_result.to_transport.put_items = [
        (
            CallRpcCommand(message=rpc_message, options={}),
            None,
        ),
    ]
    # fmt: on

    # No exception
    try:
        assert_rpc_not_called("api.bad_rpc")
    except AssertionError as e:
        assert False, f"{method_name} incorrectly raised an assertion error: {e}"

    with pytest.raises(AssertionError):
        assert_rpc_not_called("api.rpc")


@pytest.mark.parametrize(
    "method_name", ["get_rpc_messages", "getRpcMessages"], ids=["snake", "camel"]
)
def test_mock_result_get_rpc_messages(mock_result: testing.MockResult, method_name):
    get_rpc_messages = getattr(mock_result, method_name)

    rpc_message1 = RpcMessage(api_name="api", procedure_name="rpc")
    rpc_message2 = RpcMessage(api_name="api", procedure_name="rpc")
    # fmt: off
    mock_result.mocker_context.rpc_result.to_transport.put_items = [
        (
            CallRpcCommand(message=rpc_message1, options={}),
            None,
        ), (
            CallRpcCommand(message=rpc_message2, options={}),
            None,
        ),
    ]
    # fmt: on

    assert get_rpc_messages() == [rpc_message1, rpc_message2]


@pytest.mark.parametrize(
    "method_name", ["get_rpc_messages", "getRpcMessages"], ids=["snake", "camel"]
)
def test_mock_result_get_rpc_messages_filtered(mock_result: testing.MockResult, method_name):
    get_rpc_messages = getattr(mock_result, method_name)

    rpc_message1 = RpcMessage(api_name="api", procedure_name="rpc1")
    rpc_message2 = RpcMessage(api_name="api", procedure_name="rpc2")
    # fmt: off
    mock_result.mocker_context.rpc_result.to_transport.put_items = [
        (
            CallRpcCommand(message=rpc_message1, options={}),
            None,
        ), (
            CallRpcCommand(message=rpc_message2, options={}),
            None,
        ),
    ]
    # fmt: on

    assert get_rpc_messages("api.rpc2") == [rpc_message2]


@pytest.mark.parametrize(
    "property_name", ["event_names_fired", "eventNamesFired"], ids=["snake", "camel"]
)
def test_mock_result_event_names_fired(mock_result: testing.MockResult, property_name):
    event1 = EventMessage(api_name="api1", event_name="event1")
    event2 = EventMessage(api_name="api2", event_name="event2")
    # fmt: off
    mock_result.mocker_context.event.to_transport.put_items = [
        (
            SendEventCommand(message=event1, options={}),
            None,
        ), (
            SendEventCommand(message=event2, options={}),
            None,
        ),
    ]
    # fmt: on
    assert getattr(mock_result, property_name) == ["api1.event1", "api2.event2"]


@pytest.mark.parametrize(
    "property_name", ["rpc_names_called", "rpcNamesCalled"], ids=["snake", "camel"]
)
def test_mock_result_rpc_names_called(mock_result: testing.MockResult, property_name):
    rpc_message1 = RpcMessage(api_name="api", procedure_name="rpc")
    rpc_message2 = RpcMessage(api_name="api2", procedure_name="rpc2")
    # fmt: off
    mock_result.mocker_context.rpc_result.to_transport.put_items = [
        (
            CallRpcCommand(message=rpc_message1, options={}),
            None,
        ), (
            CallRpcCommand(message=rpc_message2, options={}),
            None,
        ),
    ]
    # fmt: on
    assert getattr(mock_result, property_name) == ["api.rpc", "api2.rpc2"]


def test_bus_mocker_event_ok(dummy_bus, dummy_api):
    dummy_bus.client.register_api(dummy_api)
    with testing.BusMocker(dummy_bus) as bus_mocker:
        bus_mocker.mock_event_firing("my.dummy.my_event")
        dummy_bus.my.dummy.my_event.fire(field="x")
        bus_mocker.assert_events_fired("my.dummy.my_event")


def test_bus_mocker_event_not_mocked(dummy_bus, dummy_api):
    dummy_bus.client.register_api(dummy_api)
    with testing.BusMocker(dummy_bus) as bus_mocker:
        # We don't call mock_event_firing, so we get an error here
        with pytest.raises(AssertionError):
            dummy_bus.my.dummy.my_event.fire(field="x")


def test_bus_mocker_event_mocking_disabled(dummy_bus, dummy_api):
    dummy_bus.client.register_api(dummy_api)
    with testing.BusMocker(dummy_bus, require_mocking=False) as bus_mocker:
        # We don't call mock_event_firing, but we've disabled mocking so that is ok
        dummy_bus.my.dummy.my_event.fire(field="x")
        bus_mocker.assert_events_fired("my.dummy.my_event")


def test_bus_mocker_event_mocking_disabled_but_mocked_anyway(dummy_bus, dummy_api):
    dummy_bus.client.register_api(dummy_api)
    with testing.BusMocker(dummy_bus, require_mocking=False) as bus_mocker:
        bus_mocker.mock_event_firing("my.dummy.my_event")
        dummy_bus.my.dummy.my_event.fire(field="x")
        bus_mocker.assert_events_fired("my.dummy.my_event")


def test_bus_mocker_rpc_ok(dummy_bus):
    with testing.BusMocker(dummy_bus) as bus_mocker:
        bus_mocker.mock_rpc_call("api.rpc", result=1)
        result = dummy_bus.api.rpc()
        assert result == 1
        bus_mocker.assert_rpc_called("api.rpc")


def test_bus_mocker_rpc_not_mocked(dummy_bus):
    with testing.BusMocker(dummy_bus) as bus_mocker:
        # We don't call mock_rpc_call, so we get an error here
        with pytest.raises(AssertionError):
            dummy_bus.api.rpc()


def test_bus_mocker_rpc_mocking_disabled(dummy_bus):
    with testing.BusMocker(dummy_bus, require_mocking=False) as bus_mocker:
        # We don't call mock_rpc_call, but we've disabled mocking so that is ok
        result = dummy_bus.api.rpc()
        # No mocking setup, so RPCs just return None
        assert result == None
        bus_mocker.assert_rpc_called("api.rpc")


def test_bus_mocker_rpc_mocking_disabled_but_mocked_anyway(dummy_bus):
    with testing.BusMocker(dummy_bus, require_mocking=False) as bus_mocker:
        bus_mocker.mock_rpc_call("api.rpc", result=1)
        result = dummy_bus.api.rpc()
        assert result == 1
        bus_mocker.assert_rpc_called("api.rpc")
