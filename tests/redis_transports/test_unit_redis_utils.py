from datetime import datetime, timezone

import pytest

from lightbus.transports.redis import (
    redis_stream_id_subtract_one,
    redis_steam_id_to_datetime,
    datetime_to_redis_steam_id,
    redis_stream_id_add_one,
)

pytestmark = pytest.mark.unit


def test_redis_stream_id_subtract_one():
    assert redis_stream_id_subtract_one("1514028809812-0") == "1514028809811-9999"
    assert redis_stream_id_subtract_one("1514028809812-10") == "1514028809812-9"
    assert redis_stream_id_subtract_one("0000000000000-0") == "0000000000000-0"


def test_redis_stream_id_add_one():
    assert redis_stream_id_add_one("1514028809812-0") == "1514028809812-1"
    assert redis_stream_id_add_one("1514028809812-10") == "1514028809812-11"
    assert redis_stream_id_add_one("0000000000000-0") == "0000000000000-1"


def test_redis_steam_id_to_datetime():
    assert redis_steam_id_to_datetime("0000000000000-0") == datetime(
        1970, 1, 1, 0, 0, tzinfo=timezone.utc
    )
    assert redis_steam_id_to_datetime("0000000000000-1") == datetime(
        1970, 1, 1, 0, 0, 0, 1, tzinfo=timezone.utc
    )
    assert redis_steam_id_to_datetime("1514028809812-10") == datetime(
        2017, 12, 23, 11, 33, 29, 812_010, tzinfo=timezone.utc
    )
    assert redis_steam_id_to_datetime(b"0000000000000-0") == datetime(
        1970, 1, 1, 0, 0, tzinfo=timezone.utc
    )


def test_datetime_to_redis_steam_id():
    assert (
        datetime_to_redis_steam_id(datetime(1970, 1, 1, 0, 0, tzinfo=timezone.utc))
        == "0000000000000-0"
    )
    # Microseconds don't get added on as the sequence number
    assert (
        datetime_to_redis_steam_id(datetime(1970, 1, 1, 0, 0, 0, 99, tzinfo=timezone.utc))
        == "0000000000000-0"
    )
    assert (
        datetime_to_redis_steam_id(datetime(2017, 12, 23, 11, 33, 29, 812_010, tzinfo=timezone.utc))
        == "1514028809812-0"
    )
