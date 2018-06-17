!!! note
    We recommend read the [concepts](concepts.md) section before continuing
    as this will give you a useful overview before delving into the details
    below.

Events are definied as properties on your API classes. They are
useful when:

1. You wish to allow non-authoritative services to receive
   information without needing to concern yourself
   with their implementation
2. You wish the authoritative service to perform a known
   task in the background

**RPCs provide at-least-once delivery semantics.** Given this,
your event handlers should be [idempotent].

## Definition

You can define events using the `lightbus.Event` class. For example,
you could define the following bus.py in your authenication service:

```python3
# auth_service/bus.py
from lightbus import Api, Event


class AuthApi(Api):
    user_created = Event(parameters=('username', 'email'))
    user_updated = Event(parameters=('username', 'new_email'))
    user_deleted = Event(parameters=('username'))

    class Meta:
        name = 'auth'
```

## Firing

You can fire events as follows:

```python3
# Anywhere in your code

# Import your project's bus instance
from bus import bus

bus.auth.user_created.fire(username='adam', password='adam@example.com')
```

You can also fire events asynchronously using asyncio:

```python3
# Anywhere in your code

# Import your project's bus instance
from bus import bus

await bus.auth.user_created.fire_async(
    username='adam',
    password='adam@example.com'
)
```

## Listening

Listening for events is typically a long-running background
activity, and is therefore dealt with by the `lightbus run`
command.

You can setup these listeners in another services' bus.py file
as follows:

```python3
# other_service/bus.py
import lightbus

bus = lightbus.create()
user_db = {}


def handle_created(username, email):
    user_db[username] = email
    print(user_db)


def handle_updated(username, email):
    user_db[username] = email
    print(user_db)


def handle_deleted(username, email):
    user_db.pop(username)
    print(user_db)


def before_server_start():
    # before_server_start() is called on lightbus startup,
    # this allows you to setup your listeners.

    bus.auth.user_created.listen(handle_created)
    bus.auth.user_updated.listen(handle_updated)
    bus.auth.user_deleted.listen(handle_deleted)

```


## Type hints

Type hinting for events is slightly different to that for RPCs.
Firstly, events do not have return values. We also define the
parameter types differently.

Extending our example from above:

```python3
# auth_service/bus.py
from lightbus import Api, Event, Parameter


class AuthApi(Api):
    user_created = Event(parameters=(
        Parameter('username', str),
        Parameter('new_email', str),
    ))
    user_updated = Event(parameters=(
        Parameter('username', str),
        Parameter('new_email', str),
    ))
    user_deleted = Event(parameters=(
        Parameter('username', str),
    ))
    # You can also set default values
    permission_changed = Event(parameters=(
        Parameter('username', str),
        Parameter('is_admin', bool, default=False),
    ))
```

This will provide the same benefits as with RPCs. Firstly,
incoming values will be best-effort casted to the given type
(see [typing](typing.md)).

Secondly, type hints will be used in creating your bus' schema. This
schema is shared on the bus and will be used to validate incoming
and outgoing messages. This allows a number of errors to be caught
automatically.

For example, when firing the `permission_changed` event above, Lightbus
will check to ensure the `username` parameter is present and a string.
Lightbus will also ensure `is_admin` is a boolean *if* it is present,
otherwise it will default to `False`. These checks will be
performed before the message is written to the bus.

Conversely, upon Lightbus receiving a message from the bus it will
also apply the same checks. An error will be raised if the checks fail.

TODO: Link to config docs re validate

## Event streaming

## Best practices

### Event naming

### Architecture

### Parameter values

See the [parameter values](rpcs.md#parameter-values) section on the
RPC page.

[idempotent]: https://en.wikipedia.org/wiki/Idempotence
