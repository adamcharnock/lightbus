# File: ./another_service/bus.py
import lightbus
from lightbus.utilities.async_tools import block

bus = lightbus.create()


def handle_new_user(event, username, email):
    # bus.client.enabled = True
    block(bus.auth.check_password.call_async(username="admin", password="secret"), timeout=2)
    # bus.client.enabled = False

    print(f"A new user was created in the authentication service:")
    print(f"    Username: {username}")
    print(f"    Email: {email}")


@bus.client.on_start()
def bus_start(**kwargs):
    bus.auth.user_registered.listen(handle_new_user, listener_name="print_on_new_registration")
