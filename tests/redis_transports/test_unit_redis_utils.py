from datetime import datetime

import pytest

from lightbus.transports.redis import redis_stream_id_subtract_one, redis_steam_id_to_datetime

pytestmark = pytest.mark.unit


def test_redis_stream_id_subtract_one():
    assert redis_stream_id_subtract_one("1514028809812-0") == "1514028809811-9999"
    assert redis_stream_id_subtract_one("1514028809812-10") == "1514028809812-9"
    assert redis_stream_id_subtract_one("0000000000000-0") == "0000000000000-0"


def test_redis_steam_id_to_datetime():
    assert redis_steam_id_to_datetime("0000000000000-0") == datetime(1970, 1, 1, 0, 0)
    assert redis_steam_id_to_datetime("0000000000000-1") == datetime(1970, 1, 1, 0, 0, 0, 1)
    assert redis_steam_id_to_datetime("1514028809812-10") == datetime(
        2017, 12, 23, 11, 33, 29, 812010
    )
    assert redis_steam_id_to_datetime(b"0000000000000-0") == datetime(1970, 1, 1, 0, 0)
