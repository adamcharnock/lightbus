import asyncio

from typing import List

from lightbus import RpcTransport, Api, RpcMessage
from lightbus.client.docks.base import BaseDock
from lightbus.client.utilities import queue_exception_checker
from lightbus.client import commands
from lightbus.exceptions import TransportIsClosed
from lightbus.transports.pool import TransportPool
from lightbus.utilities.async_tools import cancel
from lightbus.utilities.singledispatch import singledispatchmethod


class RpcResultDock(BaseDock):
    """ Takes internal Lightbus commands and performs interactions with the RPC & Result transport
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.consumer_tasks = set()

    @singledispatchmethod
    async def handle(self, command):
        raise NotImplementedError(f"Did not recognise command {command.__class__.__name__}")

    @handle.register
    async def handle_consume_rpcs(self, command: commands.ConsumeRpcsCommand):
        """Worker wishes for incoming RPC results to be listened for and processed"""
        # Not all APIs will necessarily be served by the same transport, so group them
        # accordingly
        api_names_by_transport = self.transport_registry.get_rpc_transport_pools(command.api_names)

        for rpc_transport, transport_api_names in api_names_by_transport:
            transport_apis = list(map(self.api_registry.get, transport_api_names))

            task = asyncio.ensure_future(
                self._consume_rpcs_with_transport(rpc_transport=rpc_transport, apis=transport_apis)
            )
            task.add_done_callback(queue_exception_checker(self.error_queue))
            self.consumer_tasks.add(task)

    @handle.register
    async def handle_call_rpc(self, command: commands.CallRpcCommand):
        """Client wishes to call a remote RPC"""
        api_name = command.message.api_name

        rpc_transport = self.transport_registry.get_rpc_transport_pool(api_name)
        result_transport = self.transport_registry.get_result_transport_pool(api_name)

        # TODO: Perhaps move return_path out of RpcMessage, as this feels a little gross
        command.message.return_path = result_transport.get_return_path(command.message)

        await rpc_transport.call_rpc(command.message, options=command.options)

    @handle.register
    async def handle_send_result(self, command: commands.SendResultCommand):
        """Worker wishes to sent a result back to a client"""
        raise NotImplementedError()

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
        result_transport = self.transport_registry.get_result_transport_pool(
            command.message.api_name
        )

        # TODO: Is it going to be an issue that we don't clear up this task?
        #       It may also get destroyed when it goes out of scope
        task = asyncio.ensure_future(
            self._result_listener(
                result_transport=result_transport,
                timeout=timeout,
                rpc_message=command.message,
                return_path=command.message.return_path,
                options=command.options,
                result_queue=command.destination_queue,
            )
        )
        task.add_done_callback(queue_exception_checker(self.error_queue))

    async def _result_listener(
        self,
        result_transport: TransportPool,
        timeout: float,
        rpc_message: RpcMessage,
        return_path: str,
        options: dict,
        result_queue: asyncio.Queue,
    ):
        try:
            result = await asyncio.wait_for(
                result_transport.receive_result(
                    rpc_message=rpc_message, return_path=return_path, options=options
                ),
                timeout=timeout,
            )
        except asyncio.TimeoutError as e:
            await result_queue.put(e)
        else:
            await result_queue.put(result)

    @handle.register
    async def handle_close(self, command: commands.CloseCommand):
        """Client or worker wishes us to close down"""
        await cancel(*self.consumer_tasks)
        for rpc_transport in self.transport_registry.get_all_rpc_transport_pools():
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
                await self.producer.send(commands.RpcCallReceived(message=rpc_message)).wait()
