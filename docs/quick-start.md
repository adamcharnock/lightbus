## Requirements

Before continuing, ensure you have completed the following steps detailed in
the [installation section](installation.md):

* Installed Python 3.6 or above
* Installed Lightbus
* Running a Redis server (built from the Redis unstable branch)

## Anatomy lesson

When using Lightbus you will still run your various services
as normal. For web-based software this will likely include one or more
processes to handle web traffic (e.g. Django, Flask).
You may or may not also have some other processes running for other purposes.

In addition to this, Lightbus will have its own process started via
`lightbus run`.

While the roles of these processes are not strictly defined, in most
circumstances their use should break down as follows:

* **Lightbus processes** – Respond to remote procedure calls, listen for
  and handle events.
* **Other processes (web etc)** – Perform remote procedure calls, fire events

The starting point for the lightbus process is a `bus.py` file. You
should create this in your project root. You can also configure
where Lightbus looks for this module using the `--bus` option or
by setting the `LIGHTBUS_MODULE` environment variable.

## Define your API

First we will define the API the lightbus will serve.
Create the following in a `bus.py` file:

```python3
# bus.py
from lightbus import Api


class AuthApi(Api):

    class Meta:
        name = 'auth'

    def check_password(self, username, password):
        return username == 'admin' and password == 'secret'

```

You should now be able to startup Lightbus as follows:

```
lightbus run
```

Lightbus will output some logging data which will include a list of
APIs in its registry, including your new `auth` API:

![lightbus run output][lightbus-run]

Leave Lightbus running and open a new terminal window for the next stage.

## Remote procedure calls

With Lightbus running, open a new terminal window and create a file named
`call_procedure.py` in the same directory as your `bus.py`. The
`call_procedure.py` file name is arbitrary, it simply allows us to
experiment with accessing the bus.

```python3
# call_procedure.py
import lightbus

# Create a bus object
bus = lightbus.create()

# Call the check_password() procedure on our auth API
valid = bus.auth.check_password(
    username='admin',
    password='secret'
)

# Show the result
if valid:
    print('Password valid!')
else:
    print('Oops, bad username or password')
```

## Events

Events allow services to broadcast a message to any other services that
care to listen. Events are fired by the service which 'owns' the API and
received by any Lightbus service, which can include the owning service itself
(as we do below).

The owning service can be more accurately referred to as the
*authoritative service*. The authoritative service is the service
which contains the class definition within its codebase. Lightbus only
allows the authoritative service to fire events for an API. Any service can
listen for any event.

We will talk more about this in [concepts](concepts.md). For now let's look
at some code. Below we modify our `AuthApi` in `bus.py` to add a `user_registered`
event. We also use the `before_server_start()` hook to setup a listener for
that event:


```python3
# bus.py
from lightbus import Api, Event

class AuthApi(Api):
    user_registered = Event(parameters=('username', 'email'))

    class Meta:
        name = 'auth'

    def check_password(self, username, password):
        return username == 'admin' and password == 'secret'


def before_server_start(bus):
    # before_server_start() is called on lightbus startup,
    # this allows you to setup your listeners.

    # Call send_welcome_email() when we receive the user_registered event
    bus.auth.user_registered.listen(send_welcome_email)


def send_welcome_email(event_message, username, email):
    # In our example we'll just print something to the console,
    # rather than send an actual email
    print(f'Subject: Welcome to our site, {username}')
    print(f'To: {email}')
```

Now create `fire_event.py`, this will fire the event on the bus.
As with the previous example, this file name is arbitrary.
In a real-world scenario this code may live in your web application's
user registration success handler.

```python3
# fire_event.py
import lightbus

# Import the AuthApi to make it available to Lightbus
from .bus import AuthApi

# Create a bus object
bus = lightbus.create()

# Fire the event. There is no return value when firing events
bus.auth.user_registered.fire(
    username='admin',
    email='admin@example.com'
)
```

There a two important differences here:

1. We call `bus.auth.user_registered.fire()` to fire the `user_registered` event on
   the `auth` API. This will place the event onto the bus to be consumed any
   listening services.
2. We import the `AuthApi` class. This registers it with Lightbus, thereby indicating
   we are the authoritative service for this API and can therefore fire events upon it.

## Further reading

This quickstart has covered the basics of Lightbus, and has hopefully given you a
good starting point. Reading through the remainder of this documentation should give you
a wider awareness of the features available and underlying concepts.


[lightbus-run]: static/images/quickstart-lightbus-run.png
