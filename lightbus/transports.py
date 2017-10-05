import asyncio
from typing import Any

from lightbus.message import Message

__all__ = ['BrokerTransport', 'ResultTransport', 'DebugBrokerTransport', 'DebugResultTransport']


class BrokerTransport(object):

    async def call_rpc(self, api, name, kwargs, result_info, priority=0):
        """Publish a call to a remote procedure"""
        pass

    async def consume_rpcs(self, api):
        """Consume RPC calls for the given API"""
        pass

    async def send_event(self, api, name, kwargs):
        """Publish an event"""
        pass

    async def consume_events(self, api):
        """Consume RPC events for the given API"""
        pass


class ResultTransport(object):

    def get_result_info(self, api, name, kwargs, priority=0) -> dict:
        return {}

    async def send(self, client_message: Message, result):
        """Send a result back to the caller

        Args:
            client_message (): The original message received from the client
            result (): The result to be sent back to the client
        """
        pass

    async def receive(self, client_message: Message):
        """Receive the response for the given message

        Args:
            client_message (): The original message sent to the server
        """
        pass


class DebugBrokerTransport(BrokerTransport):

    async def call_rpc(self, api, name, kwargs, result_info, priority=0):
        """Publish a call to a remote procedure"""
        pass

    async def consume_rpcs(self, api):
        """Consume RPC calls for the given API"""
        await asyncio.sleep(1)
        return Message(body=b'incoming rpc')

    async def send_event(self, api, name, kwargs):
        """Publish an event"""
        pass

    async def consume_events(self, api):
        """Consume RPC events for the given API"""
        pass


class DebugResultTransport(ResultTransport):

    def get_result_info(self, api, name, kwargs, priority=0) -> dict:
        return {
            'debug-result-transport': 'hello',
        }

    async def send(self, client_message: Message, result: Any):
        # TODO: Encoding etc
        message = Message(body=result)
        print('Sending response to "{}": {}'.format(client_message.body, message.body))

    async def receive(self, client_message: Message):
        pass
