# fire_event.py
import lightbus
import lightbus_examples.ex02_quickstart_event.bus

# Create a bus object
bus = lightbus.create()

# Fire the event. There is no return value when firing events
bus.auth.user_registered.fire(
    username='admin',
    email='admin@example.com'
)
