Events are defined as properties on your [API](apis.md) classes.
Events are useful when:

1. You need asynchronous communication between services
1. You wish to loosely-couple your services
1. You need a service to perform a task in the background

See [event considerations] in the explanations section for further
discussion.

**Events provide at-least-once delivery semantics.** Given this,
your event handlers should be [idempotent].

## Defining events

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

## Firing events

You can fire events as follows:

```python3
# Anywhere in your code

# Import your project's bus instance
from bus import bus

bus.auth.user_created.fire(username='adam', password='adam@example.com')
```

## Firing events (asynchronously)

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

## Listening for events

Listening for events is typically a long-running background
activity, and is therefore dealt with by the `lightbus run`
command.

You can setup these listeners in another services' `bus` module
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

    bus.auth.user_created.listen(
        handle_created,
        listener_name="user_created"
    )
    bus.auth.user_updated.listen(
        handle_updated,
        listener_name="user_updated"
    )
    bus.auth.user_deleted.listen(
        handle_deleted,
        listener_name="user_deleted"
    )

```

Specifying `listener_name` for each listener ensures each
listener will receive its own copy of the `auth.user_deleted` event.
See the [events explanation page] for further discussion.

## Listening for events (asynchronously)

Event listener setup shown above should normally happen very quickly,
however the process does still require some input/output.

You can therefore modify the above example to setup an event
listener asynchronously using the `list_async()` method:

```python3
# ...snipped from above example

# Note that before_server_start() is defined as async
async def before_server_start():

    # We await the listen_async() method
    await bus.auth.user_created.listen_async(
        handle_created,
        listener_name="user_created"
    )
```

## Type hints

See the [typing reference](typing.md).


[idempotent]: https://en.wikipedia.org/wiki/Idempotence
[event considerations]: /explanation/events.md#considerations
[events explanation page]: /explanation/events.md
