import asyncio
import logging

from lightbus.utilities import setup_dev_logging

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    setup_dev_logging()

    import lightbus

    bus = lightbus.create()

    async def register_listener():
        await asyncio.sleep(1)

        def test_listener(**kwargs):
            logger.warning('Listener called! {}'.format(kwargs))
        await bus.my_company.auth.user_registered.listen_asyn(test_listener)

    loop = asyncio.get_event_loop()
    asyncio.ensure_future(register_listener(), loop=loop)

    # bus.run_forever()

    bus.my_company.auth.check_password(
        username='admin',
        password='secret'
    )
