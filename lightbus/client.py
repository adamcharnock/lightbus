import asyncio
import logging
from typing import Optional, Callable

import lightbus.transports.direct
from lightbus.exceptions import InvalidClientNodeConfiguration
from lightbus.log import L, Bold, escape_codes
from lightbus.utilities import setup_dev_logging

logger = logging.getLogger(__name__)


class ClientNode(object):

    def __init__(self, name: str, *, parent: Optional['ClientNode'],
                 on_call: Callable, on_listen: Callable, on_fire: Callable):
        if not parent and name:
            raise InvalidClientNodeConfiguration("Root client node may not have a name")
        self.name = name
        self.parent = parent
        self.on_call = on_call
        self.on_listen = on_listen
        self.on_fire = on_fire

    def __getattr__(self, item):
        return ClientNode(name=item, parent=self,
                          on_call=self.on_call, on_listen=self.on_listen, on_fire=self.on_fire)

    def __str__(self):
        return self.fully_qualified_name

    def __repr__(self):
        return '<ClientNode {}>'.format(self.fully_qualified_name)

    def __call__(self, **kwargs):
        coroutine = self.on_call(api_name=self.api_name, name=self.name, kwargs=kwargs)

        # TODO: Make some utility code for this
        loop = asyncio.get_event_loop()
        val = loop.run_until_complete(asyncio.wait_for(coroutine, timeout=1))
        return val

    async def asyn(self, **kwargs):
        return await self.on_call(api_name=self.api_name, name=self.name, kwargs=kwargs)

    def listen(self, listener):
        return self.on_listen(api_name=self.api_name, name=self.name, listener=listener)

    def fire(self, **kwargs):
        return self.on_fire(api_name=self.api_name, name=self.name, kwargs=kwargs)

    def ancestors(self, include_self=False):
        parent = self
        while parent is not None:
            if parent != self or include_self:
                yield parent
            parent = parent.parent

    @property
    def api_name(self):
        path = [node.name for node in self.ancestors(include_self=False)]
        path.reverse()
        return '.'.join(path[1:])

    @property
    def fully_qualified_name(self):
        path = [node.name for node in reversed(self.ancestors(include_self=True))]
        path.reverse()
        return '.'.join(path[1:])


if __name__ == '__main__':
    setup_dev_logging()

    # Make sure we make the AuthApi available
    import lightbus.serve

    bus = lightbus.Bus(
        rpc_transport=lightbus.transports.RedisRpcTransport(),
        result_transport=lightbus.transports.RedisResultTransport(),
    )
    client = bus.client()
    client.my_company.auth.check_password(
        username='admin',
        password='secret'
    )
