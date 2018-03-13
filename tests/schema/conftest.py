import pytest

from lightbus import Schema, RedisSchemaTransport, DebugSchemaTransport, Api, Event, Parameter


@pytest.fixture
def schema(redis_pool):
    return Schema(
        schema_transport=RedisSchemaTransport(redis_pool=redis_pool),
    )


@pytest.fixture
def dummy_schema(redis_pool):
    return Schema(
        schema_transport=DebugSchemaTransport(),
    )


@pytest.fixture
def TestApi():
    class TestApi(Api):
        my_event = Event([Parameter('field', bool)])

        class Meta:
            name = 'my.test_api'

        def my_proc(self, field: bool=True) -> str:
            pass

    return TestApi
