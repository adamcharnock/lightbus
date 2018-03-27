from lightbus import Api


class AuthApi(Api):

    class Meta:
        name = 'auth'

    def check_password(self, username, password):
        return username == 'admin' and password == 'secret'
