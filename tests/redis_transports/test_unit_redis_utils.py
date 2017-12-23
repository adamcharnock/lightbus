import asyncio

import pytest

from lightbus.message import RpcMessage
from lightbus.transports.redis import redis_stream_id_subtract_one


def test_redis_stream_id_subtract_one():
    assert redis_stream_id_subtract_one('1514028809812-0') == '1514028809811-9999'
    assert redis_stream_id_subtract_one('1514028809812-10') == '1514028809812-9'
    assert redis_stream_id_subtract_one('0000000000000-0') == '0000000000000-0'

