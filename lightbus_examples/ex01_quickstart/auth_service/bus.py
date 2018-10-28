# File: auth_service/bus.py
import lightbus

# Create your service's bus client. You can import this elsewere
# in your service's codebase in order to access the bus
bus = lightbus.create()


class AuthApi(lightbus.Api):
    user_registered = lightbus.Event(parameters=("username", "email"))

    class Meta:
        name = "auth"

    def check_password(self, username, password):
        return username == "admin" and password == "secret"


# Register this API with Lightbus. Lightbus will respond to
# remote procedure calls for registered APIs, as well as allow you
# as the developer to fire events on any registered APIs.
bus.client.register_api(AuthApi())
