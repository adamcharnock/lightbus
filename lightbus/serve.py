import lightbus
import lightbus.transports.debug
from lightbus.api import Api, Event
from lightbus.utilities import setup_dev_logging


class AuthApi(Api):
    user_registered = Event(arguments=['username'])

    class Meta:
        name = 'my_company.auth'

    def check_password(self, username, password):
        return username == 'admin' and password == 'secret'


def main():
    setup_dev_logging()

    bus = lightbus.Bus(
        rpc_transport=lightbus.transports.RedisRpcTransport(),
        result_transport=lightbus.transports.RedisResultTransport(),
        event_transport=lightbus.transports.RedisEventTransport(),
    ).root()
    bus.my_company.auth.user_registered.fire(username='adam')

    # bus.run()


if __name__ == '__main__':
    main()
