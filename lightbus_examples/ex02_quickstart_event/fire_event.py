# fire_event.py

# Unlike in ex01, we can also get our bus straight for bus.py.
# This also makes this API available locally (i.e. we are authoritative)
# thereby allowing us to fire events on it.
from .bus import bus

# Fire the event. There is no return value when firing events
bus.auth.user_registered.fire(username="admin", email="admin@example.com")
