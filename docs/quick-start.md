## Requirements

Before continuing, ensure you have completed the following steps detailed in 
the [installation section](installation.md):

* Installed Python 3.6 or above
* Installed Lightbus
* Running a Redis server (built from the Redis unstable branch)

## Anatomy lesson

When using Lightbus you will still run your various processes
as normal. For web-based software this will likely include one or more
processes to handle web traffic (e.g. Django or Flask).
You may or may not also have some other processes running for other purposes.  

In addition to this, Lightbus will have its own process started via 
`lightbus run`.

While the roles of these processes are not strictly defined, in most
circumstances their use should break down as follows:

* **Lightbus processes** – Respond to remote procedure calls, listen for 
  and handle events.
* **Other processes (web etc)** – Perform remote procedure calls, fire events

The starting point for the lightbus process is a `bus.py` file. You 
should create this in your project root.

## Define your API

First we will define the API the lightbus will serve. 
Create the following in a `bus.py` file:

```python3
# bus.py
from lightbus import Api, Event


class AuthApi(Api):
    user_registered = Event(parameters=['username', 'email'])

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

![alt text][lightbus-run]

Leave Lightbus running and open a new terminal window for the next stage. 

## Call the API

With Lightbus running, open a new terminal window and create a file named 
`call_procedure.py` in the same directory as your `bus.py`. The 
`call_procedure.py` file name is arbitrary, it is simply to allow us to 
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



[lightbus-run]: static/images/quickstart-lightbus-run.png
