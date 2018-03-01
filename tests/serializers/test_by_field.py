import pytest

from lightbus.message import EventMessage
from lightbus.serializers.by_field import ByFieldMessageSerializer, ByFieldMessageDeserializer

pytestmark = pytest.mark.unit


def test_by_field_serializer():
    serializer = ByFieldMessageSerializer()
    serialized = serializer(EventMessage(
        api_name='my.api',
        event_name='my_event',
        kwargs={'field': 'value'}
    ))
    assert serialized == {
        'api_name': 'my.api',
        'event_name': 'my_event',
        ':field': '"value"',
    }


def test_by_field_deserializer():
    deserializer = ByFieldMessageDeserializer(EventMessage)
    message = deserializer({
        'api_name': 'my.api',
        'event_name': 'my_event',
        ':field': '"value"',
    })
    assert message.api_name == 'my.api'
    assert message.event_name == 'my_event'
    assert message.kwargs == {'field': 'value'}
