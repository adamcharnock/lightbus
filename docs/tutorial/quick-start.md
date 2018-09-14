## 2.1 Requirements

Before continuing, ensure you have completed the following steps detailed in
the [installation section](/tutorial/installation.md):

* Installed Python 3.6 or above
* Installed Lightbus
* Running Redis

Optionally, you can read some additional *explanation* in the
[anatomy lesson] and [concepts] sections.

## 2.2 Define your API

First we will define the API the lightbus will serve.
Create the following in a `bus.py` file:

```python3
# bus.py
import lightbus

# Create your service's bus client. You can import this elsewere
# in your service's codebase in order to access the bus
bus = lightbus.create()

class AuthApi(lightbus.Api):

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

## 2.3 Remote procedure calls

With Lightbus running, open a new terminal window and create a file named
`call_procedure.py` in the same directory as your `bus.py`. This is
just a regular Python script which will use to call the bus.

```python3
# call_procedure.py
import lightbus

# We'll assume we're writing a new service here,
# so create a new bus client
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

Running this script should show you the following:

    $ python3 ./call_procedure.py
    Password valid!

Looking at the other terminal window you have open you should see that
Lightbus has also reported that it is handled a remote procedure call.

## 2.4 Events

Events allow services to broadcast a message to any other services which
cares to listen. Events are fired by the service which 'owns' the API and
received by any Lightbus service, which can include the owning service itself
(as we do below).

The owning service can be more accurately referred to as the
*[authoritative] service* for the given API. The authoritative service is the service
which contains the API's class definition within its codebase. Lightbus only
allows the authoritative service to fire events for an API. Any service can
listen for any event.

We will talk more about this in [concepts](/explanation/concepts.md). For now let's look
at some code. Below we modify our `AuthApi` in `bus.py` to add a `user_registered`
event. We also use the `before_server_start()` hook to setup a listener for
that event:


```python3
# bus.py
import lightbus

bus = lightbus.create()

class AuthApi(lightbus.Api):
    user_registered = lightbus.Event(parameters=('username', 'email'))

    class Meta:
        name = 'auth'

    def check_password(self, username, password):
        return username == 'admin' and password == 'secret'


def before_server_start():
    # before_server_start() is called on lightbus startup,
    # this allows you to setup your listeners.

    # Call send_welcome_email() when we receive the user_registered event
    bus.auth.user_registered.listen(
        send_welcome_email,
        listener_name="send_welcome_email"
    )


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

# Here we assume we're writing another module within
# the same auth service, not creating a new service.
# Therefore import our bus client from bus.py
from bus import bus

# Fire the event. There is no return value when firing events
bus.auth.user_registered.fire(
    username='admin',
    email='admin@example.com'
)
```


Here we call `bus.auth.user_registered.fire()` to fire the `user_registered` event on
the `auth` API. This will place the event onto the bus to be consumed any
listening services.

## 2.5 Next

This was a simple example to get you started. The [worked example] considers
a more realistic scenario involving multiple services.


[lightbus-run]: /static/images/quickstart-lightbus-run.png
[anatomy lesson]: /explanation/anatomy-lesson.md
[concepts]: /explanation/concepts.md
[worked example]: worked-example.md
[authoritative]: /explanation/apis.md#authoritativenon-authoritative-apis
