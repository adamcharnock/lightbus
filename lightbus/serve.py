import lightbus
from lightbus.api import Api
from lightbus.utilities import setup_dev_logging


class AuthApi(Api):

    class Meta:
        name = 'my_company.auth'

    def check_password(self, username, password):
        return username == 'admin' and password == 'secret'


def main():
    setup_dev_logging()

    bus = lightbus.Bus(
        broker_transport=lightbus.DebugBrokerTransport(),
        result_transport=lightbus.DebugResultTransport()
    )
    api = AuthApi()
    bus.serve(api)


if __name__ == '__main__':
    main()
