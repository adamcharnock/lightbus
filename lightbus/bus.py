from typing import Optional

import asyncio


class Bus(object):

    def __init__(self, broker_transport, result_transport):
        self.broker_transport = broker_transport
        self.result_transport = result_transport

    def __getattr__(self, item):
        return ClientNode(name=str(item), bus=self, parent=None)


class ClientNode(object):

    def __init__(self, name: str, bus: Bus, parent: Optional['ClientNode']):
        self.name = name
        self.bus = bus
        self.parent = parent

    def __call__(self, *args, **kwargs):
        """Synchronous call"""
        loop = asyncio.get_event_loop()
        val = loop.run_until_complete(asyncio.wait_for(self.asyn(*args, **kwargs), timeout=1))
        loop.close()
        return val

    async def asyn(self, *args, **kwargs):
        """Asynchronous call"""
        await asyncio.sleep(0.5)
        return 'Called'

    def __getattr__(self, item):
        return ClientNode(name=str(item), bus=self.bus, parent=self)

    def __str__(self):
        path = [node.name for node in self.ancestors(include_self=True)]
        path.reverse()
        return '.'.join(path)

    def __repr__(self):
        return '<ClientNode {}>'.format(self.name)

    def ancestors(self, include_self=False):
        parent = self
        while parent is not None:
            if parent != self or include_self:
                yield parent
            parent = parent.parent


if __name__ == '__main__':
    bus = Bus(broker_transport=None, result_transport=None)
    print(bus.adam.foo.bar.hello())
