from lightbus import Api, Event


class AuthApi(Api):
    user_registered = Event(parameters=['username', 'email'])

    class Meta:
        name = 'auth'

    def check_password(self, username, password):
        return username == 'admin' and password == 'secret'


def before_server_start(bus):
    ...
