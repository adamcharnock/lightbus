from time import sleep

import lightbus

bus = lightbus.create()


class AuthApi(lightbus.Api):
    user_registered = lightbus.Event()
    request_send_email = lightbus.Event()
    something_else = lightbus.Event()

    class Meta:
        name = "auth"

    def check_password(self):
        bus.auth.something_else.fire()


bus.client.register_api(AuthApi())


@bus.client.on_start()
def on_startup(**kwargs):
    @bus.client.every(seconds=1)
    def constantly_register_users():
        bus.auth.user_registered.fire()

    def handle_new_user(event):
        bus.auth.request_send_email.fire()
        bus.auth.check_password()

    bus.auth.user_registered.listen(handle_new_user, listener_name="ex98_nested_bus_calls")
