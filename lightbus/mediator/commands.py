import asyncio
import logging

from typing import NamedTuple, Optional, List, Tuple

from lightbus.api import Api
from lightbus.message import EventMessage, RpcMessage, ResultMessage

logger = logging.getLogger(__name__)


class SendEventCommand(NamedTuple):
    message: EventMessage
    options: dict = {}


class ConsumeEventsCommand(NamedTuple):
    events: List[Tuple[str, str]]
    listener_name: str
    destination_queue: asyncio.Queue
    options: dict = {}


class AcknowledgeEventCommand(NamedTuple):
    message: EventMessage
    options: dict = {}


class SendRpcCommand(NamedTuple):
    message: RpcMessage


class PublishApiSchemaCommand(NamedTuple):
    api: Api


class CloseCommand(NamedTuple):
    pass


TRANSPORT_COMMANDS = {SendEventCommand, SendRpcCommand, PublishApiSchemaCommand, CloseCommand}


class ReceiveEventCommand(NamedTuple):
    message: EventMessage
    listener_name: str


class ReceiveResultCommand(NamedTuple):
    message: ResultMessage


class ReceiveSchemaUpdateCommand(NamedTuple):
    schema: dict


class ShutdownCommand(NamedTuple):
    exception: Optional[BaseException]


CLIENT_COMMANDS = {
    ReceiveEventCommand,
    ReceiveResultCommand,
    ReceiveSchemaUpdateCommand,
    ShutdownCommand,
}
