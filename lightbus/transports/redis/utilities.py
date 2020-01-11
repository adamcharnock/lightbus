import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional, Mapping

import aioredis
from aioredis import Redis, ConnectionsPool, ReplyError, ConnectionClosedError

from aioredis.util import decode

from lightbus.transports.base import EventMessage
from lightbus.exceptions import LightbusException, TransportIsClosed, LightbusShutdownInProgress
from lightbus.utilities.frozendict import frozendict

logger = logging.getLogger("lightbus.transports.redis")


def redis_stream_id_subtract_one(message_id):
    """Subtract one from the message ID

    This is useful when we need to xread() events inclusive of the given ID,
    rather than exclusive of the given ID (which is the sensible default).
    Only use when one can tolerate an exceptionally slim risk of grabbing extra events.
    """
    milliseconds, n = map(int, message_id.split("-"))
    if n > 0:
        n = n - 1
    elif milliseconds > 0:
        milliseconds = milliseconds - 1
        n = 9999
    else:
        # message_id is '0000000000000-0'. Subtracting one
        # from this is neither possible, desirable or useful.
        return message_id
    return "{:13d}-{}".format(milliseconds, n)


def redis_stream_id_add_one(message_id):
    """Add one to the message ID

    This is useful when we need to xrange() events exclusive of the given ID,
    rather than inclusive of the given ID (which is the sensible default).
    There is no chance of missing events with this method.
    """
    milliseconds, n = map(int, message_id.split("-"))
    return f"{milliseconds:013d}-{n + 1}"


def normalise_since_value(since):
    """Take a 'since' value and normalise it to be a redis message ID"""
    if not since:
        return "$"
    elif hasattr(since, "timestamp"):  # datetime
        # Create message ID: "<milliseconds-timestamp>-<sequence-number>"
        return f"{round(since.timestamp() * 1000):013d}-0"
    else:
        return since


def redis_steam_id_to_datetime(message_id):
    message_id = decode(message_id, "utf8")
    milliseconds, seq = map(int, message_id.split("-"))
    # Treat the sequence value as additional microseconds to ensure correct sequencing
    microseconds = (milliseconds % 1000 * 1000) + seq
    dt = datetime.utcfromtimestamp(milliseconds // 1000).replace(
        microsecond=microseconds, tzinfo=timezone.utc
    )
    return dt


def datetime_to_redis_steam_id(dt: datetime) -> str:
    timestamp = round(dt.timestamp() * 1000)
    # We ignore microseconds here as using them in the sequence
    # number would make the the sequence number non sequential
    return f"{timestamp:013d}-0"


class InvalidRedisPool(LightbusException):
    pass


class RedisEventMessage(EventMessage):
    def __init__(self, *, stream: str, native_id: str, consumer_group: Optional[str], **kwargs):
        super(RedisEventMessage, self).__init__(**kwargs)
        self.stream = stream
        self.native_id = native_id
        self.consumer_group = consumer_group

    @property
    def datetime(self):
        return redis_steam_id_to_datetime(self.native_id)

    def get_metadata(self) -> dict:
        metadata = super().get_metadata()
        metadata["stream"] = self.stream
        metadata["native_id"] = self.native_id
        metadata["consumer_group"] = self.consumer_group
        return metadata


class RedisTransportMixin:
    connection_parameters: dict = {"address": "redis://localhost:6379", "maxsize": 100}
    _redis_pool = None

    def set_redis_pool(
        self,
        redis_pool: Optional[Redis],
        url: str = None,
        connection_parameters: Mapping = frozendict(),
    ):
        self._redis_pool = None
        self._closed = False

        if not redis_pool:
            # Connect lazily using the provided parameters

            self.connection_parameters = self.connection_parameters.copy()
            self.connection_parameters.update(connection_parameters)
            if url:
                self.connection_parameters["address"] = url
        else:
            # Use the provided connection

            if isinstance(redis_pool, (ConnectionsPool,)):
                # If they've passed a raw pool then wrap it up in a Redis object.
                # aioredis.create_redis_pool() normally does this for us.
                redis_pool = Redis(redis_pool)
            if not isinstance(redis_pool, (Redis,)):
                raise InvalidRedisPool(
                    "Invalid Redis connection provided: {}. If unsure, use aioredis.create_redis_pool() to "
                    "create your redis connection.".format(redis_pool)
                )
            if not isinstance(redis_pool._pool_or_conn, (ConnectionsPool,)):
                raise InvalidRedisPool(
                    "The provided redis connection is backed by a single connection, rather than a "
                    "pool of connections. This will lead to lightbus deadlocks and is unsupported. "
                    "If unsure, use aioredis.create_redis_pool() to create your redis connection."
                )

            # Determine the connection parameters from the given pool
            # (we will need these in other to create new pools for other threads)
            self.connection_parameters = dict(
                address=redis_pool.address,
                db=redis_pool.db,
                password=redis_pool._pool_or_conn._password,
                encoding=redis_pool.encoding,
                minsize=redis_pool._pool_or_conn.minsize,
                maxsize=redis_pool._pool_or_conn.maxsize,
                ssl=redis_pool._pool_or_conn._ssl,
                parser=redis_pool._pool_or_conn._parser_class,
                timeout=redis_pool._pool_or_conn._create_connection_timeout,
                connection_cls=redis_pool._pool_or_conn._connection_cls,
            )

            self._redis_pool = redis_pool

    async def connection_manager(self) -> Redis:
        if self._closed:
            # This was first caught when the state plugin tried to send a
            # message to the bus on upon the after_worker_stopped stopped event.
            raise TransportIsClosed(
                "Transport has been closed. Connection to Redis is no longer available."
            )

        if not self._redis_pool:
            self._redis_pool = await aioredis.create_redis_pool(**self.connection_parameters)

        try:
            internal_pool = self._redis_pool._pool_or_conn
            if hasattr(internal_pool, "size") and hasattr(internal_pool, "maxsize"):
                if internal_pool.size == internal_pool.maxsize:
                    logging.critical(
                        "Redis pool has reached maximum size. It is possible that this will recover normally, "
                        "you may have more event listeners than connections available to the Redis pool. "
                        "You can increase the redis pool size by specifying the `maxsize` "
                        "parameter in each of the Redis transport configuration sections. Current maxsize is: {}"
                        "".format(self.connection_parameters.get("maxsize"))
                    )

            return await self._redis_pool
        except aioredis.PoolClosedError:
            raise LightbusShutdownInProgress(
                "Redis connection pool has been closed. Assuming shutdown in progress."
            )

    async def close(self):
        self._closed = True
        if self._redis_pool:
            await self._close_redis_pool()

    async def _close_redis_pool(self):
        self._redis_pool.close()
        await self._redis_pool.wait_closed()
        # In Python >= 3.7 the redis connections do not actually get
        # immediately closed following the above call to wait_closed().
        # If you need to be sure the the connections have closed the
        # a call to `await asyncio.sleep(0.001)` does the trick
        del self._redis_pool

    def __str__(self):
        if self._redis_pool:
            conn = self._redis_pool.connection
            return f"redis://{conn.address[0]}:{conn.address[1]}/{conn.db}"
        else:
            return self.connection_parameters.get("address", "Unknown URL")


async def retry_on_redis_connection_failure(fn, *, args=tuple(), retry_delay: int, action: str):
    """Decorator to repeatedly call fn in case of Redis connection problems"""
    while True:
        try:
            return await fn(*args)
        except (ConnectionClosedError, ConnectionResetError):
            # ConnectionClosedError is from aioredis. However, sometimes the connection
            # can die outside of aioredis, in which case we get a builtin ConnectionResetError.
            logger.warning(
                f"Redis connection lost while {action}, reconnecting "
                f"in {retry_delay} seconds..."
            )
            await asyncio.sleep(retry_delay)
        except ConnectionRefusedError:
            logger.warning(
                f"Redis connection refused while {action}, retrying " f"in {retry_delay} seconds..."
            )
            await asyncio.sleep(retry_delay)
        except ReplyError as e:
            if "LOADING" in str(e):
                logger.warning(
                    f"Redis server is still loading, retrying " f"in {retry_delay} seconds..."
                )
                await asyncio.sleep(retry_delay)
            else:
                raise
