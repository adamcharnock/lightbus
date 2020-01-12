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

You can define events using the `Event` class. For example,
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

You can setup event listeners in your services' `bus` module
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


@bus.client.on_start()
def on_start():
    # Bus client has started up, so register our listeners

    bus.auth.user_created.listen(
        handle_created,
        listener_name="user_created"
    )
    bus.auth.user_updated.listen(
        handle_updated,
        listener_name="user_updated",
    )
    bus.auth.user_deleted.listen(
        handle_deleted,
        listener_name="user_deleted"
    )

```

Specifying `listener_name` is required in order to ensure 
each listeners receives all events it is due.
See the [events explanation page] for further discussion.

---

You cannot have two listeners with the same name for the same API.
For example, this will **not** work (it will raise a `DuplicateListenerName` exception:

```python3
bus.competitor_prices.changed.listen(
    send_price_alerts,
    listener_name="price_listener",
)

### ERROR ##
# This will raise a DuplicateListenerName exception because  
# we have already created a listener named 'price_listener'
# on the 'competitor_prices' API (above).
bus.competitor_prices.changed.listen(
    update_prices,
    listener_name="price_listener",  # ⇠ DuplicateListenerName exception
)
```

This restriction only applies to listeners within the same service.

### Errors in event listeners

By default, the Lightbus worker will exit if an event listener encounters an error. 
The event being handled will remain unacknowledged, and will be re-attempted 
after [acknowledgement_timeout] seconds.

You can customise this behaviour by passing the `on_error` parameter to `listen()`.
Possible values are:

* `lightbus.OnError.SHUTDOWN` (Default) - Shutdown the Lightbus work upon encountering an error
* `lightbus.OnError.ACKNOWLEDGE_AND_LOG` (Default) - Acknowledge erroring messages as being successfully processed,
  and log the error.

For example:

```python3
# Taken from the above example

# Worker shutdown on error (the default)
bus.auth.user_created.listen(
    handle_created,
    listener_name="user_created",
    on_error=lightbus.OnError.SHUTDOWN,
)

# Or, log the error and acknowlegde the message as being processed
bus.auth.user_created.listen(
    handle_created,
    listener_name="user_created",
    on_error=lightbus.OnError.ACKNOWLEDGE_AND_LOG,
)
```

## Type hints

See the [typing reference](typing.md).


[idempotent]: https://en.wikipedia.org/wiki/Idempotence
[event considerations]: ../explanation/events.md#considerations
[events explanation page]: ../explanation/events.md
[acknowledgement_timeout]: transport-configuration.md#acknowledgement_timeout
