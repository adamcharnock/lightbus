import lightbus

bus = lightbus.create()


class AuthApi(lightbus.Api):
    user_registered = lightbus.Event(
        parameters=(
            lightbus.Parameter("username", str),
            lightbus.Parameter("email", str),
            lightbus.Parameter("is_admin", bool, default=False),
        )
    )

    class Meta:
        name = "auth"

    def check_password(self, username: str, password: str) -> bool:
        return username == "admin" and password == "secret"
