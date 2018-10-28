# call_procedure.py
import lightbus

# Import the bus from our bus.py file
from lightbus_examples.ex02_quickstart_event.bus import bus

# Call the check_password() procedure on our auth API
valid = bus.auth.check_password(username="admin", password="secret")

# Show the result
if valid:
    print("Password valid!")
else:
    print("Oops, bad username or password")
