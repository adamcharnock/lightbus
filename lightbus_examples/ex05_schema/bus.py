from lightbus import Api, Event, Parameter

class AuthApi(Api):
    user_registered = Event(parameters=(
        Parameter('username', str),
        Parameter('email', str),
        Parameter('is_admin', bool, default=False),
    ))

    class Meta:
        name = 'auth'

    def check_password(self, username: str, password: str) -> bool:
        return username == 'admin' and password == 'secret'
