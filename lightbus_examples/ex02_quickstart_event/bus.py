# bus.py
from lightbus import Api, Event

class AuthApi(Api):
    user_registered = Event(parameters=('username', 'email'))

    class Meta:
        name = 'auth'

    def check_password(self, username, password):
        return username == 'admin' and password == 'secret'


def send_welcome_email(username, email):
    # In our example we'll just print something to the console,
    # rather than send an actual email
    print(f'Subject: Welcome to our site, {username}')
    print(f'To: {email}')


def before_server_start(bus):
    # Called on lightbus startup, allows you to setup your listeners
    bus.auth.user_registered.listen(send_welcome_email)
