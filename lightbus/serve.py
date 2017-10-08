import logging

import lightbus
import lightbus.transports.debug
from lightbus.api import Api
from lightbus.log import LBullets, L, Bold
from lightbus.utilities import setup_dev_logging


class AuthApi(Api):

    class Meta:
        name = 'my_company.auth'

    def check_password(self, username, password):
        return username == 'admin' and password == 'secret'


def main():
    setup_dev_logging()

    bus = lightbus.Bus(
        rpc_transport=lightbus.transports.debug.DebugRpcTransport(),
        result_transport=lightbus.transports.debug.DebugResultTransport()
    )
    api = AuthApi()
    bus.serve(api)


if __name__ == '__main__':
    main()
