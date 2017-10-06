import lightbus


class Api(object):

    class Meta:
        name = None

    async def call(self, procedure_name, kwargs):
        # TODO: Handling code for sync/async method calls (if we want to support both)
        return getattr(self, procedure_name)(**kwargs)


class AuthApi(Api):

    class Meta:
        name = 'my_company.auth'

    def check_password(self, username, password):
        return username == 'admin' and password == 'secret'


def main():
    bus = lightbus.Bus(
        broker_transport=lightbus.DebugBrokerTransport(),
        result_transport=lightbus.DebugResultTransport()
    )
    api = AuthApi()
    bus.serve(api)


if __name__ == '__main__':
    main()
