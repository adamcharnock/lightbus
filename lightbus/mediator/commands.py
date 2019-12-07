import asyncio
import logging

from typing import NamedTuple, Optional

from lightbus.api import Api
from lightbus.message import EventMessage, RpcMessage, ResultMessage

logger = logging.getLogger(__name__)


class SendEventCommand(NamedTuple):
    message: EventMessage
    on_done: asyncio.Event
    options: dict = {}


class SendRpcCommand(NamedTuple):
    message: RpcMessage
    on_done: asyncio.Event


class PublishApiSchemaCommand(NamedTuple):
    api: Api
    on_done: asyncio.Event


class CloseCommand(NamedTuple):
    on_done: asyncio.Event


TRANSPORT_COMMANDS = {SendEventCommand, SendRpcCommand, PublishApiSchemaCommand, CloseCommand}


class ReceiveEventCommand(NamedTuple):
    message: EventMessage
    on_done: asyncio.Event


class ReceiveResultCommand(NamedTuple):
    message: ResultMessage
    on_done: asyncio.Event


class ReceiveSchemaUpdateCommand(NamedTuple):
    schema: dict
    on_done: asyncio.Event


class ShutdownCommand(NamedTuple):
    exception: Optional[BaseException]


CLIENT_COMMANDS = {
    ReceiveEventCommand,
    ReceiveResultCommand,
    ReceiveSchemaUpdateCommand,
    ShutdownCommand,
}
