import logging

from lightbus.utilities import setup_dev_logging

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    setup_dev_logging()

    import lightbus.transports
    import lightbus.serve

    bus = lightbus.Bus(
        rpc_transport=lightbus.transports.RedisRpcTransport(),
        result_transport=lightbus.transports.RedisResultTransport(),
        event_transport=lightbus.transports.DebugEventTransport(),
    )

    bus.run()

    # client = bus.client()
    # client.my_company.auth.user_registered.fire(username='foo')

    # client.my_company.auth.check_password(
    #     username='admin',
    #     password='secret'
    # )
