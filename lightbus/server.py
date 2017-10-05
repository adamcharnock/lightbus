import asyncio
import traceback

import lightbus


class Server(object):

    def __init__(self, bus: 'lightbus.Bus'):
        self.bus = bus

    def run_forever(self):
        loop = asyncio.get_event_loop()
        asyncio.ensure_future(self._handle_exceptions(self.consume), loop=loop)
        loop.run_forever()
        loop.close()

    async def consume(self):
        while True:
            message = await self.bus.consume_rpcs(api=None)
            result = await self.call_rpc(message)
            await self.bus.send_result(client_message=message, result=result)

    async def call_rpc(self, message):
        # TODO
        return 'RPC result'

    async def _handle_exceptions(self, fn, *args, **kwargs):
        try:
            await fn(*args, **kwargs)
        except asyncio.CancelledError:
            pass
        except Exception:
            traceback.print_exc()


def main():
    bus = lightbus.Bus(
        broker_transport=lightbus.DebugBrokerTransport(),
        result_transport=lightbus.DebugResultTransport()
    )
    bus.serve()


if __name__ == '__main__':
    main()
