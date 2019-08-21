# File: ./another_service/bus.py
import lightbus

bus = lightbus.create()


def handler_new_user(event, username, email):
    print(f"A new user was created in the authentication service:")
    print(f"    Username: {username}")
    print(f"    Email: {email}")


@bus.client.on_start()
def bus_start(**kwargs):
    bus.auth.user_registered.listen(handler_new_user, listener_name="print_on_new_registration")
