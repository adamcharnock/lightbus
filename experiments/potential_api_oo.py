Api = object  # Base API class
Event = object  # A simple event/signal system

############
# LONG FORM: Separate classes for definition & implementation allow
#            putting definitions in a common python package available
#            across all apps
############

# client.py

class AuthApi(Api):
    user_registered = Event()
    user_account_closed = Event()

    def get_user(self, username: str) -> dict:
        pass

    def check_password(self, password: str) -> bool:
        pass

api = AuthApi.as_client()


# server.py


class AuthImplementation(AuthApi):  # Inherits from client definition

    def get_user(self, username: str) -> dict:
        # Actual implementation
        return {
            'name': 'Test User',
            'email': 'test@example.com',
        }

    def check_password(self, password: str) -> dict:
        return (password == 'Passw0rd!')

api = AuthImplementation.as_server()


##############
# ALTERNATIVE: Can combine both definitions if separation is not required.
##############

# client_server.py

class AuthImplementation(Api):
    user_registered = Event()
    user_account_closed = Event()

    def get_user(self, username: str) -> dict:
        # Actual implementation
        return {
            'name': 'Test User',
            'email': 'test@example.com',
        }

    def check_password(self, password: str) -> dict:
        return password == 'Passw0rd!'

client = AuthImplementation.as_client()
server = AuthImplementation.as_server()


# Pros:
#   - Personal preference: I find this more readable
#   - IDE's will warn about definition/implementation signatures not matching
#   - Makes our implementation different(/easier?)
#   - Has the option of being DRY where client/server separation is not required
# Cons:
#   - Not DRY in it's long form
#   - Forcing an OO design

#######################
# Additional thoughts #
#######################

# We could have a top level apis.py, much like Django's urls.py:

# /apis.py

apis = [
    SparePartsApi.as_server(),  # This is the spare parts application, so serve its API
    AuthApi.as_client(),  # We need the Auth API in order to authenticate clients
    CustomersApi.as_client(only=['support_ticket_opened']),  # Select only certain events
    MetricsApi.as_client(exclude=['page_view']),  # Filter out high-volume events we don't care about
]

# Warren would be able to read this list of APIs and setup the necessary AMQP bindings.
# Each API gets its own queue to avoid high activity on one API blocking all others.

