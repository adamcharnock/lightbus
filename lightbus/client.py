import asyncio
import logging
from typing import Optional

import lightbus.transports.direct
from lightbus.exceptions import InvalidClientNodeConfiguration
from lightbus.log import L, Bold, escape_codes
from lightbus.utilities import setup_dev_logging

logger = logging.getLogger(__name__)


class ClientNode(object):

    def __init__(self, name: str, bus: 'Bus', parent: Optional['ClientNode']):
        if not parent and name:
            raise InvalidClientNodeConfiguration("Root client node may not have a name")
        self.name = name
        self.bus = bus
        self.parent = parent

    def __call__(self, **kwargs):
        """Synchronous call"""
        loop = asyncio.get_event_loop()
        val = loop.run_until_complete(asyncio.wait_for(self.asyn(**kwargs), timeout=1))
        return val

    async def asyn(self, **kwargs):
        """Asynchronous call"""
        api_name = self.path(include_self=False)
        logger.info("📞  Calling {}.{} with kwargs: {}".format(api_name, self.name, kwargs))

        result = await self.bus.call_rpc_remote(
            api_name=api_name,
            name=self.name,
            kwargs=kwargs
        )
        logger.debug("Call {}.{} completed with result: {}".format(api_name, self.name, result))
        return result

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
    client.my_company.auth.check_password(
        username='admin',
        password='secret'
    )
    client.my_company.auth.check_password(
        username='admin',
        password='secret'
    )

