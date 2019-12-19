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


class CallRpcCommand(NamedTuple):
    message: RpcMessage
    options: dict = {}


class ConsumeRpcsCommand(NamedTuple):
    api_names: List[str]
    options: dict = {}


class ExecuteRpcCommand(NamedTuple):
    """An RPC call has been received and must be executed locally"""

    message: RpcMessage


class PublishApiSchemaCommand(NamedTuple):
    api: Api


class CloseCommand(NamedTuple):
    pass


TRANSPORT_COMMANDS = {SendEventCommand, CallRpcCommand, PublishApiSchemaCommand, CloseCommand}


class SendResultCommand(NamedTuple):
    rpc_message: RpcMessage
    message: ResultMessage


class ReceiveResultCommand(NamedTuple):
    message: RpcMessage
    destination_queue: asyncio.Queue
    options: dict


class ReceiveSchemaUpdateCommand(NamedTuple):
    schema: dict


class ShutdownCommand(NamedTuple):
    exception: Optional[BaseException]


CLIENT_COMMANDS = {ReceiveResultCommand, ReceiveSchemaUpdateCommand, ShutdownCommand}
