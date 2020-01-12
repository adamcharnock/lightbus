import asyncio
import logging
from datetime import datetime
from typing import Sequence, Tuple, List, Dict, AsyncGenerator, TYPE_CHECKING

from lightbus.transports.base import ResultTransport, RpcTransport, EventTransport, SchemaTransport
from lightbus.message import RpcMessage, EventMessage, ResultMessage

if TYPE_CHECKING:
    # pylint: disable=unused-import,cyclic-import
    from lightbus.client import BusClient

logger = logging.getLogger(__name__)


class DebugRpcTransport(RpcTransport):
    async def call_rpc(self, rpc_message: RpcMessage, options: dict):
        """Publish a call to a remote procedure"""
        logger.debug("Faking dispatch of message {}".format(rpc_message))

    async def consume_rpcs(self, apis) -> Sequence[RpcMessage]:
        """Consume RPC calls for the given API"""
        logger.debug("Faking consumption of RPCs. Waiting 100ms before issuing fake RPC call...")
        await asyncio.sleep(0.1)
        logger.debug("Issuing fake RPC call")
        return self._get_fake_messages()

    def _get_fake_messages(self):
        return [
            RpcMessage(
                api_name="my_company.auth",
                procedure_name="check_password",
                kwargs=dict(username="admin", password="secret"),  # nosec
            )
        ]


class DebugResultTransport(ResultTransport):
    async def get_return_path(self, rpc_message: RpcMessage) -> str:
        return "debug://foo"

    async def send_result(
        self, rpc_message: RpcMessage, result_message: ResultMessage, return_path: str
    ):
        logger.info("Faking sending of result: {}".format(result_message))

    async def receive_result(
        self, rpc_message: RpcMessage, return_path: str, options: dict
    ) -> ResultMessage:
        logger.info("⌛ Faking listening for results. Will issue fake result in 0.5 seconds...")
        await asyncio.sleep(0.1)  # This is relied upon in testing
        logger.debug("Faking received result")

        return ResultMessage(
            result="Fake result",
            rpc_message_id=rpc_message.id,
            api_name=rpc_message.api_name,
            procedure_name=rpc_message.procedure_name,
        )


class DebugEventTransport(EventTransport):
    def __init__(self):
        self._task = None
        self._reload = False
        self._events = set()
        super().__init__()

    async def send_event(self, event_message: EventMessage, options: dict):
        """Publish an event"""
        logger.info(
            " Faking sending of event {}.{} with kwargs: {}".format(
                event_message.api_name, event_message.event_name, event_message.kwargs
            )
        )

    async def consume(
        self, listen_for: List[Tuple[str, str]], listener_name: str, **kwargs
    ) -> AsyncGenerator[EventMessage, None]:
        """Consume RPC events for the given API"""
        self._sanity_check_listen_for(listen_for)

        logger.info("⌛ Faking listening for events {}.".format(self._events))

        while True:
            await asyncio.sleep(0.1)
            yield [self._get_fake_message()]

    async def history(
        self,
        api_name,
        event_name,
        start: datetime = None,
        stop: datetime = None,
        start_inclusive: bool = True,
    ) -> AsyncGenerator[EventMessage, None]:
        yield self._get_fake_message()

    def _get_fake_message(self):
        message = EventMessage(
            api_name="my_company.auth", event_name="user_registered", kwargs={"example": "value"}
        )
        setattr(message, "datetime", datetime.now())
        return message


class DebugSchemaTransport(SchemaTransport):
    def __init__(self):
        self._schemas = {}

    async def store(self, api_name: str, schema: Dict, ttl_seconds: int):
        self._schemas[api_name] = schema

        logging.debug(
            "Debug schema transport storing schema for {} (TTL: {}): {}".format(
                api_name, ttl_seconds, schema
            )
        )

    async def load(self) -> Dict[str, Dict]:
        return self._schemas
