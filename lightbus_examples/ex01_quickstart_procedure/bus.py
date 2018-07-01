import lightbus

bus = lightbus.create()


class AuthApi(lightbus.Api):

    class Meta:
        name = "auth"

    def check_password(self, username, password):
        return username == "admin" and password == "secret"
