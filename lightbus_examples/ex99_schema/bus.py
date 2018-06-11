from lightbus import Api, Event, Parameter, create

bus = create()


class AuthApi(Api):
    user_registered = Event(parameters=[Parameter("username", str)])

    class Meta:
        name = "auth"

    def check_password(self, username: str, password: str):
        return username == "admin" and password == "secret"
