# File: ./another_service/check_password.py

# Import our service's bus client
from bus import bus

# Call the check_password() procedure on our auth API
valid = bus.auth.check_password(username="admin", password="secret")

# Show the result
if valid:
    print("Password valid!")
else:
    print("Oops, bad username or password")
