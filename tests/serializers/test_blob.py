import json

import pytest

from lightbus.message import EventMessage
from lightbus.serializers.blob import BlobMessageSerializer, BlobMessageDeserializer

pytestmark = pytest.mark.unit


def test_blob_serializer():
    serializer = BlobMessageSerializer()
    serialized = serializer(
        EventMessage(api_name="my.api", event_name="my_event", id="123", kwargs={"field": "value"})
    )
    assert json.loads(serialized) == {
        "metadata": {"api_name": "my.api", "event_name": "my_event", "id": "123", "version": 1},
        "kwargs": {"field": "value"},
    }


def test_blob_deserializer():
    deserializer = BlobMessageDeserializer(EventMessage)
    message = deserializer(
        json.dumps(
            {
                "metadata": {
                    "api_name": "my.api",
                    "event_name": "my_event",
                    "id": "123",
                    "version": 1,
                },
                "kwargs": {"field": "value"},
            }
        )
    )
    assert message.api_name == "my.api"
    assert message.event_name == "my_event"
    assert message.id == "123"
    assert message.kwargs == {"field": "value"}


def test_blob_deserializer_dict():
    deserializer = BlobMessageDeserializer(EventMessage)
    message = deserializer(
        {
            "metadata": {"api_name": "my.api", "event_name": "my_event", "id": "123", "version": 1},
            "kwargs": {"field": "value"},
        }
    )
    assert message.api_name == "my.api"
    assert message.event_name == "my_event"
    assert message.id == "123"
    assert message.kwargs == {"field": "value"}
