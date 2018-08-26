# Anatomy lesson

Lightbus provides you with two tools:

* A **client** with which to fire events and make remote procedure calls (RPCs).
  This can be used from anywhere within your Python codebase.
* A **stand-alone Lightbus process** which will listen for events,
  and respond to remote procedure calls. These processes are long running,
  and you will typically have one or more per [service].

You will need to make use of both when using Lightbus.

## The client

The client allows you to interact with the bus from within your Python
codebase. For example:

```python3
import lightbus

# Create the lightbus client
bus = lightbus.create()

# Perform a remote procedure call
is_valid = bus.auth.check_password(
    user="admin",
    password="secret"
)

# Fire an event
bus.auth.user_registered(
    user="sally",
    email="sally@example.com"
)
```

You can use this client anywhere you need to, such as:

* Within your Django/Flask views
* Within scheduled jobs
* Within Lightbus event & RPC handlers (see below)

## The Lightbus process

The Lightbus process listens for events and responds to
remote procedure calls. In order to set this up you must:

1. Create a `bus.py` file. Within this file...
1. Instantiate the `bus` client
1. Import any API definitions you wish to serve remote procedure calls for
1. Register handlers for any events you wish to listen for

For example, let's use the `auth.create_user()` remote procedure call
to create a new user every time a `customers.new_customer` event appears on the
bus:

```python3
# bus.py
import lightbus

bus = lightbus.create()

def create_user_for_customer(event_message,
                             customer_name, email):
    # We can do something locally, or call an
    # RPC, or both. Here we call an RPC.
    bus.auth.create_user(
        name=customer_name,
        email=email
    )

# Setup our listeners on startup
def before_server_start():
    # Create a new user for each new customer
    bus.customers.new_customer.listen(
        create_user_for_customer
    )
```

You start this process using the command:

    lightbus run


This will import a module named `bus` (your `bus.py` file) and wait
for events and RPCs.

While `bus` is the default module name, you can override it using the
`LIGHTBUS_MODULE` environment variable,
or the `lightbus run --bus=...` option.

## Addendum

The distinction between client & Lightbus process as described above
is convention rather than a technical requirement.

While you will need the client regardless, [you can start the Lightbus
server in a more advanced fashion](/howto/combine-processes.md).

The Lightbus process executes within the `asyncio` event loop. You
can therefore merge the Lightbus process with any other process which
also runs within the `asyncio` event loop.

This does however add complexity, and the rewards are likely limited.
Therefore only pursue this path if you are sure it suits your
particular needs.

[service]: concepts.md#service
