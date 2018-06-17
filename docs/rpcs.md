!!! note
    We recommend read the [concepts](concepts.md) section before continuing
    as this will give you a useful overview before delving into the details
    below.

Remote procedures calls (RPCs) are defined as methods on your API
classes. They are useful when either:

* You require information from a service
* You wish to wait until a remote procedure has completed an action

**RPCs provide at-most-once delivery semantics.** If you need
at-least-once semantics you should consder using events instead.

## Definition

As covered in previous sections, you define RPCs as follows:

```python3
# bus.py
from lightbus import Api


class AuthApi(Api):

    class Meta:
        name = 'auth'

    def check_password(self, username, password):
        return username == 'admin' and password == 'secret'

    def reset_password(self, username):
        reset_users_password_somehow(username)

    def get_user(self, username):
        return get_user(username)

    def promote_to_admin(username):
        user = get_user(username)
        user.admin = True
        user.save()
```

## Calling

RPCs are called simply as follows:

```python3
is_valid = bus.auth.check_password(username="adam", password="secr3t")
```

You can also perform the call asynchronously using asyncio:

```python3
is_valid = await bus.auth.check_password.call_async(
    username="adam",
    password="secr3t"
)
```

## Type hints

Specifying type hints on your RPCs will provide a number of benefits.

Firstly, incoming values will be best-effort casted to the given type
(see [typing](typing.md)).

Secondly, type hints will be used in creating your bus' schema. This
schema is shared on the bus and will be used to validate incoming
and outgoing messages. This allows a number of errors to be caught
automatically. For example:

```python3
    ...
    def get_user(self, username: str) -> User:
        return get_user(username)
```

This will validate that:

* The incoming `username` value is present and is a string
* The returned data matches the annotations on the `User` class

The above checks will also be applied when calling the `get_user()`
RPC, and when receiving it's response.

TODO: Link to config docs re validate

## Best practices

### RPC implementation

It is best to keep your RPCs simple and easy to understand.
In anything but the simplest service it will probably be best to
use the API definition as a presentational layer which wraps up
the business logic located elsewhere.

If you find business logic creeping into your RPC definitions,
consider factoring it out and invoking it from the RPC definition.

For smaller services this will be less important, but as functionality
is shared and used elsewhere within your service you may find it keeps
your code more managable.

This also leaves your RPCs definitions free do any API-specific
legwork such as data marshalling. For example, converting incoming natural keys
(e.g. usernames) into the primary keys (i.e. user IDs) which your
service's may use internally.

### Architecture & coupling

RPCs often represent a tight coupling between your code and the
service you are calling. This may be acceptable to you, but it is
worth being aware of potential pitfalls:

* Failures & timeouts may occurr, which should ideally be
  handled gracefully
* Modifications to the remote RPC may require updates to the
  code which calls the RPC
* RPCs incur much greater overhead than regular function calls.
  Utility functions that use RPCs should therefore make it clear
  that they will be incurring this overhead (either through
  naming convention or documentation)
* The more services call to for a given action, the less
  reliable the action will typically be. (i.e. if any service is
  unavailable the action will potentially fail)

Using events will provide a different set of trade-offs, and
ultimately you will need to decide on what is right for
your particular scenario.

### Parameter values

When deciding the values your RPC should receive, consider:

* Can this value become out of date? For example, an entire user object
  could become out of date, whereas a username or user ID would not.
* Packing and unpacking large data structures is computationally expensive.
* Will the meaning of the value change over time? For example, the meaning of 'today' or
  'now' will change, but the meaning of a specific date & time will remain the same.

## Limitations

RPCs can only be called with keyword arguments. For example:

```python3
# Raises an InvalidParameters exception
result = bus.auth.check_password('admin', 'secret')  # ERROR

# Success
result = bus.auth.check_password(username='admin', password='secret')
```

