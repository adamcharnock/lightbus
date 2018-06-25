import lightbus
import lightbus.creation

bus = lightbus.creation.create()


class AuthApi(lightbus.Api):

    class Meta:
        name = "auth"

    def check_password(self, username, password):
        return username == "admin" and password == "secret"
