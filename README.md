# What is Lightbus?

[![Codacy Badge](https://api.codacy.com/project/badge/Coverage/f5e5fd4eeb57462b80e2a99e957b7baa)](https://app.codacy.com/gh/adamcharnock/lightbus/dashboard)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-v2.0%20adopted-ff69b4.svg)](https://lightbus.org/reference/code-of-conduct/)

Lightbus allows your backend processes to communicate, run background
tasks, and expose internal APIs.

Lightbus uses Redis as its underlying transport, although support for
other platforms may eventually be added.

Lightbus requires Python 3.9 or above.

**Full documentation can be found at https://lightbus.org**

## Designed for ease of use

Lightbus is designed with developers in mind. The syntax aims to be
intuitive and familiar, and common problems are caught with clear and
helpful error messages.

For example, a naïve authentication API:

``` python3
class AuthApi(Api):
    user_registered = Event(parameters=('username', 'email'))

    class Meta:
        name = 'auth'

    def check_password(self, user, password):
        return (
            user == 'admin'
            and password == 'secret'
        )
```

This can be called as follows:

``` python3
import lightbus

bus = lightbus.create()

bus.auth.check_password(
    user='admin',
    password='secret'
)
# Returns true
```

You can also listen for events:

``` python3
import lightbus

bus = lightbus.create()

def send_signup_email(event_message,
                      username, email):
    send_mail(email,
        subject=f'Welcome {username}'
    )

@bus.client.on_start()
def bus_start(client):
    bus.auth.user_registered.listen(
        send_signup_email
    )
```

**To get started checkout the documentation at https://lightbus.org.**
