import asyncio
from typing import Any

from lightbus.message import RpcMessage, EventMessage, ResultMessage

__all__ = ['BrokerTransport', 'ResultTransport', 'DebugBrokerTransport', 'DebugResultTransport']


class BrokerTransport(object):
    # TODO: BrokerTransport is probably the wrong name, as it implies how it should be implemented

    async def call_rpc(self, api, name, kwargs, result_info, priority=0):
        """Publish a call to a remote procedure"""
        pass

    async def consume_rpcs(self, api) -> RpcMessage:
        """Consume RPC calls for the given API"""
        pass

    async def send_event(self, api, name, kwargs):
        """Publish an event"""
        pass

    async def consume_events(self, api) -> EventMessage:
        """Consume RPC events for the given API"""
        pass


class ResultTransport(object):

    def get_result_info(self, api, name, kwargs, priority=0) -> dict:
        return {}

    async def send(self, rpc_message: RpcMessage, result_message: ResultMessage):
        """Send a result back to the caller

        Args:
            rpc_message (): The original message received from the client
            result_message (): The result message to be sent back to the client
        """
        pass

    async def receive(self, rpc_message: RpcMessage) -> ResultMessage:
        """Receive the response for the given message

        Args:
            rpc_message (): The original message sent to the server
        """
        pass


class DebugBrokerTransport(BrokerTransport):

    async def call_rpc(self, api, name, kwargs, result_info, priority=0):
        """Publish a call to a remote procedure"""
        pass

    async def consume_rpcs(self, api) -> RpcMessage:
        """Consume RPC calls for the given API"""
        await asyncio.sleep(1)
        return RpcMessage(api_name='my_company.auth', procedure_name='check_password', kwargs=dict(
            username='admin',
            password='secret',
        ))

    async def send_event(self, api, name, kwargs):
        """Publish an event"""
        pass

    async def consume_events(self, api) -> EventMessage:
        """Consume RPC events for the given API"""
        pass


class DebugResultTransport(ResultTransport):

    def get_result_info(self, api, name, kwargs, priority=0) -> dict:
        return {
            'debug-result-transport': 'hello',
        }

    async def send(self, rpc_message: RpcMessage, result_message: ResultMessage):
        print('Sending result to message "{}". Result message is: {}'.format(rpc_message, result_message))

    async def receive(self, rpc_message: RpcMessage) -> ResultMessage:
        pass
