import pytest

import lightbus


@pytest.fixture
def redis_rpc_transport(new_redis_pool, server, loop):
    """Get a redis transport backed by a running redis server."""
    return lightbus.RedisRpcTransport(redis_pool=new_redis_pool())


@pytest.fixture
def redis_result_transport(new_redis_pool, server, loop):
    """Get a redis transport backed by a running redis server."""
    return lightbus.RedisResultTransport(redis_pool=new_redis_pool())


@pytest.fixture
def redis_event_transport(new_redis_pool, server, loop):
    """Get a redis transport backed by a running redis server."""
    return lightbus.RedisEventTransport(redis_pool=new_redis_pool())


@pytest.fixture
def bus(redis_rpc_transport, redis_result_transport, redis_event_transport):
    """Get a redis transport backed by a running redis server."""
    return lightbus.create(
        rpc_transport=redis_rpc_transport,
        result_transport=redis_result_transport,
        event_transport=redis_event_transport,
    )
