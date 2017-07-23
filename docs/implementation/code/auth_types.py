from lightbus import Api, Event


class AuthApi(Api):
    user_registered = Event()

    class Meta:
        name = 'my_company.auth'

    def check_password(self, username: str, password: str) -> bool:
        return username == 'admin' and password == 'secret'
