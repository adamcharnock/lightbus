import pytest

from lightbus.message import EventMessage
from lightbus.serializers.by_field import ByFieldMessageSerializer, ByFieldMessageDeserializer

pytestmark = pytest.mark.unit


def test_by_field_serializer():
    serializer = ByFieldMessageSerializer()
    serialized = serializer(
        EventMessage(
            api_name="my.api", event_name="my_event", kwargs={"field": "value"}, id="123", version=2
        )
    )
    assert serialized == {
        "api_name": "my.api",
        "event_name": "my_event",
        ":field": '"value"',
        "id": "123",
        "version": 2,
    }


def test_by_field_deserializer():
    deserializer = ByFieldMessageDeserializer(EventMessage)
    message = deserializer(
        {
            "api_name": "my.api",
            "event_name": "my_event",
            "id": "123",
            "version": "2",
            ":field": '"value"',
        },
        native_id="456",
    )
    assert message.api_name == "my.api"
    assert message.event_name == "my_event"
    assert message.id == "123"
    assert message.kwargs == {"field": "value"}
    assert message.version == 2
    assert message.native_id == "456"


def test_by_field_deserializer_empty_keys_and_values():
    deserializer = ByFieldMessageDeserializer(EventMessage)
    message = deserializer(
        {
            "api_name": "my.api",
            "event_name": "my_event",
            "id": "123",
            "version": "2",
            ":field": '"value"',
            "": "",
        },
        native_id="456",
    )
    assert message.api_name == "my.api"
    assert message.event_name == "my_event"
    assert message.id == "123"
    assert message.kwargs == {"field": "value"}
    assert message.version == 2
