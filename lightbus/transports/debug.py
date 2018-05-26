import asyncio
import logging
from typing import Sequence, Tuple, Any, Generator, List, Dict

from lightbus.transports.base import ResultTransport, RpcTransport, EventTransport, SchemaTransport
from lightbus.message import RpcMessage, EventMessage, ResultMessage


logger = logging.getLogger(__name__)


class DebugRpcTransport(RpcTransport):

    async def call_rpc(self, rpc_message: RpcMessage, options: dict):
        """Publish a call to a remote procedure"""
        logger.debug("Faking dispatch of message {}".format(rpc_message))

    async def consume_rpcs(self, api) -> Sequence[RpcMessage]:
        """Consume RPC calls for the given API"""
        logger.debug("Faking consumption of RPCs. Waiting 1 second before issuing fake RPC call...")
        await asyncio.sleep(0.1)
        logger.debug("Issuing fake RPC call")
        return self._get_fake_messages()

    def _get_fake_messages(self):
        return [
            RpcMessage(
                api_name="my_company.auth",
                procedure_name="check_password",
                kwargs=dict(username="admin", password="secret"),
            )
        ]


class DebugResultTransport(ResultTransport):

    def get_return_path(self, rpc_message: RpcMessage) -> str:
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

        return ResultMessage(result="Fake result", rpc_message_id=rpc_message.id)


class DebugEventTransport(EventTransport):

    def __init__(self):
        self._task = None
        self._reload = False
        self._events = set()

    async def send_event(self, event_message: EventMessage, options: dict):
        """Publish an event"""
        logger.info(
            " Faking sending of event {}.{} with kwargs: {}".format(
                event_message.api_name, event_message.event_name, event_message.kwargs
            )
        )

    async def fetch(
        self,
        listen_for: List[Tuple[str, str]],
        context: dict,
        loop,
        consumer_group: str = None,
        **kwargs,
    ) -> Generator[EventMessage, None, None]:
        """Consume RPC events for the given API"""

        logger.info("⌛ Faking listening for events {}.".format(self._events))

        while True:
            await asyncio.sleep(0.1)
            yield self._get_fake_message()
            yield True

    def _get_fake_message(self):
        return EventMessage(
            api_name="my_company.auth", event_name="user_registered", kwargs={"example": "value"}
        )


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
