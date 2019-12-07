import asyncio
import logging

from typing import NamedTuple, Optional

import lightbus

logger = logging.getLogger(__name__)


class SendEventCommand(NamedTuple):
    message: lightbus.EventMessage
    on_done: asyncio.Event


class SendRpcCommand(NamedTuple):
    message: lightbus.RpcMessage
    on_done: asyncio.Event


class PublishApiSchemaCommand(NamedTuple):
    api: lightbus.Api
    on_done: asyncio.Event


class CloseCommand(NamedTuple):
    on_done: asyncio.Event


TRANSPORT_COMMANDS = {SendEventCommand, SendRpcCommand, PublishApiSchemaCommand, CloseCommand}


class ReceiveEventCommand(NamedTuple):
    message: lightbus.EventMessage
    on_done: asyncio.Event


class ReceiveResultCommand(NamedTuple):
    message: lightbus.ResultMessage
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
