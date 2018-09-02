Specifying type hints allows you to validate both incoming
and outgoing messages.

Type hints are used to create your APIs' [schemas], which is shared
across your entire bus.

## Type hinting syntax for RPCs

You can provide type hints for Remote Procedure Calls as follows:

```python3
class AuthApi(lightbus.Api):

    class Meta:
        name = 'auth'

    def check_password(self, username: str, password: str) -> bool:
        return username == 'admin' and password == 'secret'
```

This will:

* Ensure the received `username` parameter is a string
* Ensure the received `password` parameter is a string
* Ensure the returned value is a boolean

This behaviour can be configure via the [validate configuration option].

## Type hinting syntax for events

Type hinting for events is slightly different to that for RPCs.
Firstly, events do not provide return values. Secondly, event
parameters are specified slightly differently.

```python3
# auth_service/bus.py
from lightbus import Api, Event, Parameter


class AuthApi(Api):
    # WITHOUT types
    user_created = Event(parameters=('username', 'email', 'is_admin'))

    # WITH types
    user_created = Event(parameters=(
        Parameter('username', str),
        Parameter('new_email', str),
        Parameter('is_admin', bool, default=False),
    ))
```

This will:

* Ensure the received `username` parameter is a string
* Ensure the received `new_email` parameter is a string
* Ensure the received `is_admin` parameter is a boolean.
  If omitted, `False` will be used.

This behaviour can be configure via the [validate configuration option].

## Data structures

The Lightbus can derive a schema from the following data structures:

* Named Tuples
* Dataclasses
* Any class defining the `__to_bus__` and `__from_bus__` methods

For each of these data structures Lightbus will:

* Encode values as JSON objects (i.e. dictionaries)
* Use the structure's type hints in generating the JSON schema...
* ... and therefore validate incoming/outgoing objects against this schema

### NamedTuple example

```python
# bus.py
from lightbus import Api
from typing import NamedTuple


class User(NamedTuple):
    username: str
    name: str
    email: str
    is_admin: bool = False

class AuthApi(Api):

    class Meta:
        name = 'auth'

    def get_user(self, username: str) -> User:
        return ...
```


### Dataclass example

[Dataclasses](https://www.python.org/dev/peps/pep-0557/)
were introduced in Python 3.7, with a
[backport available](https://pypi.org/project/dataclasses/) for
Python 3.6.

Lightbus supports dataclasses in the same way as it supports
[named tuples](#named-tuples). For example:

```python3
# bus.py
from lightbus import Api
from dataclasses import dataclass

@dataclass()
class User(object):
    username: str
    name: str
    email: str
    is_admin: bool = False

class AuthApi(Api):

    class Meta:
        name = 'auth'

    def get_user(self, username: str) -> User:
        return ...
```


### Custom class example

Lightbus can also work with classes of any type provided that:

1. The class defines a `__from_bus__(self, value: dict)` class method, which returns an instance of the class.
2. The class defines a `__to_bus__(self)` method, which returns a `dict`.


```python3
from lightbus import Api


class User(object):
    username: str
    name: str
    email: str
    is_admin: bool = False

    def do_something(self):
        pass

    @classmethod
    def __from_bus__(cls, value):
        user = cls()
        user.username = value["username"]
        user.name = value["name"]
        user.email = value["email"]
        user.is_admin = value.get("is_admin", False)
        return user

    def __to_bus__():
        return dict(
            username=username,
            name=name,
            email=email,
            is_admin=is_admin,
        )


class AuthApi(Api):

    class Meta:
        name = 'auth'

    def get_user(self, username: str) -> User:
        return ...
```

[schemas]: schema.md
[validate configuration option]: configuration.md#api-config
