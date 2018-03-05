from lightbus.api import Api, Event


class AuthApi(Api):
    user_registered = Event(parameters=['username'])

    class Meta:
        name = 'my_company.auth'

    def check_password(self, username, password):
        return username == 'admin' and password == 'secret'
