!!! note
    We recommend read the [concepts](concepts.md) section before continuing
    as this will give you a useful overview before delving into the details 
    below.

Remote procedures calls (RPCs) are defined as methods on your API 
classes. They are useful when either:

* You require information from a service
* You wish to wait until a remote procedure has completed an action

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

## Best practices

### RPC implementation

It is best to keep your RPCs simple and easy to understand. 
Use the API definition as a presentational system that wraps up 
the business logic elsewhere within service.

If you find business logic creeping into your RPC definitions, 
consider factoring it out and invoking it from the RPC definition.

For smaller services this will be less important, but as functionality 
is shared and used elsewhere within your service you may find it keeps 
you code more managable.

This also leaves your RPCs definitions free do any API-specific 
legwork such as data marshalling. For example, converting incoming natural keys 
(e.g. usernames) into the primary keys (i.e. user IDs) that your 
service's may use internally.

### Timeouts

TODO. Handle timeouts gracefully.

### Parameter values

When deciding the values your RPC should receive, consider:

* Can this value become out of date? For example, an entire user object 
  could become out of date, whereas a username or user ID would not. 
* Packing and unpacking large data structures is computationally expensive.
* Will the values meaning change over time? For example, the meaning of 'today' or 
  'now' will change, but the meaning of a specific date & time will remain the same.

### Type hints   

Assuming you have validation enabled, specifying type hints on your 
RPCs will allow a number of errors to be caught automatically. For example:

```python3
    def get_user(self, username: str) -> User:
        return get_user(username)
```

This will validate that:

* The incoming `username` value is a string
* The outgoing user object matches the annotations on the `User` class

## Limitations

RPCs can only be called with keyword arguments. For example:

```python3
# Raises an InvalidParameters exception
result = bus.auth.check_password('admin', 'secret')

# Success
result = bus.auth.check_password(username='admin', password='secret')
```

