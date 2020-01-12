import asyncio
import logging
from functools import wraps
from inspect import iscoroutinefunction

from typing import List

from lightbus import RpcTransport, Api, RpcMessage
from lightbus.client.docks.base import BaseDock
from lightbus.client.utilities import queue_exception_checker
from lightbus.client import commands
from lightbus.exceptions import TransportIsClosed
from lightbus.transports.pool import TransportPool
from lightbus.utilities.async_tools import cancel
from lightbus.utilities.internal_queue import InternalQueue
from lightbus.utilities.singledispatch import singledispatchmethod

logger = logging.getLogger(__name__)


class RpcResultDock(BaseDock):
    """ Takes internal Lightbus commands and performs interactions with the RPC & Result transport
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.consumer_tasks = set()
        self.listener_tasks = set()

    @singledispatchmethod
    async def handle(self, command):
        raise NotImplementedError(f"Did not recognise command {command.__class__.__name__}")

    @handle.register
    async def handle_consume_rpcs(self, command: commands.ConsumeRpcsCommand):
        """Worker wishes for incoming RPC results to be listened for and processed"""
        # Not all APIs will necessarily be served by the same transport, so group them
        # accordingly
        api_names_by_transport = self.transport_registry.get_rpc_transports(command.api_names)

        for rpc_transport, transport_api_names in api_names_by_transport.items():
            transport_apis = list(map(self.api_registry.get, transport_api_names))

            # fmt: off
            task = asyncio.ensure_future(queue_exception_checker(
                self._consume_rpcs_with_transport(rpc_transport=rpc_transport, apis=transport_apis),
                self.error_queue,
            ))
            # fmt: on
            self.consumer_tasks.add(task)

    @handle.register
    async def handle_call_rpc(self, command: commands.CallRpcCommand):
        """Client wishes to call a remote RPC"""
        api_name = command.message.api_name

        rpc_transport = self.transport_registry.get_rpc_transport(api_name)
        result_transport = self.transport_registry.get_result_transport(api_name)

        # TODO: Perhaps move return_path out of RpcMessage, as this feels a little gross
        command.message.return_path = await result_transport.get_return_path(command.message)

        await rpc_transport.call_rpc(command.message, options=command.options)

    @handle.register
    async def handle_send_result(self, command: commands.SendResultCommand):
        """Worker wishes to sent a result back to a client"""
        result_transport = self.transport_registry.get_result_transport(command.message.api_name)
        return await result_transport.send_result(
            command.rpc_message, command.message, command.rpc_message.return_path
        )

    @handle.register
    async def handle_receive_result(self, command: commands.ReceiveResultCommand):
        """Client wishes to receive a result from a worker"""

        # TODO: rpc_timeout is in three different places in the config!
        #       Fix this. Really it makes most sense for the use if it goes on the
        #       ApiConfig rather than having to repeat it on both the result & RPC
        #       transports.
        timeout = command.options.get(
            "timeout", self.config.api(command.message.api_name).rpc_timeout
        )
        result_transport = self.transport_registry.get_result_transport(command.message.api_name)

        logger.debug("Starting RPC result listener")
        task = asyncio.ensure_future(
            queue_exception_checker(
                self._result_listener(
                    result_transport=result_transport,
                    timeout=timeout,
                    rpc_message=command.message,
                    return_path=command.message.return_path,
                    options=command.options,
                    result_queue=command.destination_queue,
                ),
                self.error_queue,
            )
        )
        self.listener_tasks.add(task)

    async def _result_listener(
        self,
        result_transport: TransportPool,
        timeout: float,
        rpc_message: RpcMessage,
        return_path: str,
        options: dict,
        result_queue: InternalQueue,
    ):
        try:
            logger.debug("Result listener is waiting")
            result = await asyncio.wait_for(
                result_transport.receive_result(
                    rpc_message=rpc_message, return_path=return_path, options=options
                ),
                timeout=timeout,
            )
        except asyncio.TimeoutError as e:
            logger.debug("Result listener timed out")
            await result_queue.put(e)
        else:
            logger.debug("Result listener received result, putting onto result queue")
            await result_queue.put(result)
        finally:
            self.listener_tasks.discard(asyncio.current_task())

    @handle.register
    async def handle_close(self, command: commands.CloseCommand):
        """Client or worker wishes us to close down"""
        await cancel(*self.consumer_tasks)
        await cancel(*self.listener_tasks)

        for rpc_transport in self.transport_registry.get_all_rpc_transports():
            await rpc_transport.close()
        for rpc_transport in self.transport_registry.get_all_result_transports():
            await rpc_transport.close()

        await self.consumer.close()
        await self.producer.close()

    async def _consume_rpcs_with_transport(self, rpc_transport: RpcTransport, apis: List[Api]):
        while True:
            try:
                rpc_messages = await rpc_transport.consume_rpcs(apis)
            except TransportIsClosed:
                return

            for rpc_message in rpc_messages:
                await self.producer.send(commands.ExecuteRpcCommand(message=rpc_message)).wait()
