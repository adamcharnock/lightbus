import asyncio
from typing import Optional

import lightbus
from lightbus.exceptions import InvalidClientNodeConfiguration


class ClientNode(object):

    def __init__(self, name: str, bus: 'Bus', parent: Optional['ClientNode']):
        if not parent and name:
            raise InvalidClientNodeConfiguration("Root client node may not have a name")
        self.name = name
        self.bus = bus
        self.parent = parent

    def __call__(self, *args, **kwargs):
        """Synchronous call"""
        loop = asyncio.get_event_loop()
        val = loop.run_until_complete(asyncio.wait_for(self.asyn(*args, **kwargs), timeout=1))
        return val

    async def asyn(self, *args, **kwargs):
        """Asynchronous call"""
        return await self.bus.call_rpc_remote(
            api_name=self.path(include_self=False),
            name=self.name,
            kwargs={'arg': 'value'}
        )

    def __getattr__(self, item):
        return ClientNode(name=str(item), bus=self.bus, parent=self)

    def __str__(self):
        return '.'.join(self.path(include_self=True))

    def __repr__(self):
        return '<ClientNode {}>'.format(self.name)

    def ancestors(self, include_self=False):
        parent = self
        while parent is not None:
            if parent != self or include_self:
                yield parent
            parent = parent.parent

    def path(self, include_self=True):
        path = [node.name for node in self.ancestors(include_self=include_self)]
        path.reverse()
        return '.'.join(path)


if __name__ == '__main__':
    bus = lightbus.Bus(
        broker_transport=lightbus.DebugBrokerTransport(),
        result_transport=lightbus.DebugResultTransport()
    )
    client = bus.client()
    print(client.adam.foo.bar.hello())
