Lightbus processes automatically generate and share schemas for their available APIs.
These schemes can be used to validate the following: 

* Remote procedure call parameters
* Remote procedure call return values
* Event parameters

These schemas are shared using the configured `SchemaTransprt` (Redis, by default).
Each Lightbus process will monitor for any schema changes.

## Specifying types

Lightbus will create a schema by inspecting the parameters 
and [type hints] of your APIs' events and procedures. 

You can use the schema functionality without type hints, but the level of validation 
provided will be limited to ensuring parameter names match what is expected.
 
Take the following API as an example:

```python3
from lightbus import Api, Event, Parameter

class AuthApi(Api):
    # Here we specify event parameters in the long-form using Parameter().
    # This provides sufficient information for the schema to be generated
    user_registered = Event(parameters=(
        Parameter('username', str),
        Parameter('email', str),
        Parameter('is_admin', bool, default=False),
    ))

    class Meta:
        name = 'auth'
    
    # We annotate check_password() with the apropriate types
    def check_password(self, username: str, password: str) -> bool:
        return username == 'admin' and password == 'secret'
```

Create this in a ``bus.py`` and run:

```bash
$ lightbus dumpschema
```

This will dump out the auto-generated schema for the above API. See 
[schema format](#schema-format) (below) for example output.


## Supported data types

Lightbus maps Python types to JSON types. While Python-specific values can be sent using Lightbus, 
these values will arrive in their JSON form. For example, if you send a `string` then a `string` will arrive.
However, if you send the `Decimal` value `3.124`, then you will receive the `string` value `3.124` instead.

The following types are reasonably interoperable:


| Python type sent                          | JSON schema interpretation                        | Type received
| ----------------------------------------- | ------------------------------------------------- | ---------------
| `str`                                     | `string`                                          | `str`
| `int`, `float`                            | `number`                                          | `int`, `float`
| `bool   `                                 | `boolean`                                         | `bool`
| `list`, `tuple`                           | `array`                                           | `list`
| `None`                                    | `null`                                            | `None`
| `dict`, `Mapping`, etc                    | `object`                                          | `dict`
| `Mapping[str, ...]`                       | `object`, with [pattern properties] set           | `dict`
| `Tuple[A, B, C]`                          | `array` with [maxItems/minItems] and [items] set. | `list`

The following types will be successfully encoded and sent, but will arrive as their encoded equivalent:

| Python type                               | JSON Schema type                                  | Value arrives as
| ----------------------------------------- | ------------------------------------------------- | ---------------
| `bytes`, `Decimal`, `complex`             | `string`                                          | `str`
| `datetime`, `date`                        | `str`                                             | `str` (ISO 8601)
| `NamedTuple` with annotations             | `object` with [specific typed properties]         | `dict`
| `object` with annotations                 | `object` with [specific typed properties]         | `dict`

Lightbus can also handle the following:

| Python type                               | JSON Schema type                                 
| ----------------------------------------- | -------------------------------------------------
| `Any`                                     | `{}` (any value)                                  
| `Union[...]`                              | `oneOf{...}` (see [oneOf])
| `Enum`                                    | Sets [enum] property                              

## Automatic validation

By default this validation will be validated in both the 
incoming and outgoing directions. Outgoing refers to 
the dispatching of events or procedure calls to the bus.
Incoming refers to the processing of procedure calls or 
handling of received events.

You can configuring this using the ``validate`` 
[configuration](configuration.md) option.

### Validation configuration

You can configure the validation behaviour in your 
bus' `config.yaml`.

#### `validate (bool) = true`

You can enable/disable validation using a boolean true/false flag:

```coffeescript
# In config.yaml
apis:
    default:
        validate: false
```

For finer grained control you can specify individual flags for incoming/outgoing 
validation:

```coffeescript
# In config.yaml
apis:
    default:
        validate:
          outgoing: true
          incoming: false
```

#### `strict_validation (bool) = false`

If `strict_validation` is `true` then calling a procedure for which no schema exists will 
result in an error:

```coffeescript
# In config.yaml
apis:
    default:
        strict_validation: true
```

## Manual validation

TODO

## Complex types

The Lightbus can derive a schema from both named tuples and classes.

### Named Tuples

Take the following `User` named tuple:

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

Running `lightbus dumpschema` will produce the following:

```json
{
  "auth": {
    "events": {},
    "rpcs": {
      "get_user": {
        "parameters": {
          // A single required username string parameter 
          "$schema": "http://json-schema.org/draft-04/schema#",
          "title": "RPC auth.get_user() parameters",
          "type": "object"
          "additionalProperties": false,
          "properties": {
            "username": {"type": "string"}
          },
          "required": [
            "username"
          ],
        },
        
        // A complex object is returned
        "response": {
          "$schema": "http://json-schema.org/draft-04/schema#",
          "title": "RPC auth.get_user() response",
          "type": "object",
          "properties": {
            "username": {
              "type": "string"
            },
            "name": {
              "type": "string"
            },
            "email": {
              "type": "string"
            },
            "is_admin": {
              "default": false,
              "type": "boolean"
            }
          },
          "required": ["username", "name", "email"],
          "additionalProperties": false
        }
      }
    }
  }
}
```

### Classes

The same can be achieved using regular classes. However, there are some notable points:

1. Attributes prefixed with an underscore will be ignored
2. If used as a type hint for a parameter, then the class .... wait, will a json object get deserialised into a python object?
   NO, this only happens in the config. I think you'll just get a dict for a parameter value
TODO 


```coffeescript
from lightbus import Api
from typing import NamedTuple


class User(object):
    username: str
    name: str
    email: str
    is_admin: bool = False
    _internal_value: str  # ignored

    def do_something(self):
        pass

class AuthApi(Api):

    class Meta:
        name = 'auth'

    def get_user(self, username: str) -> User:
        return ...

```


## Schema format

You won't need to worry about the schema format in your day-to-day use 
of Lightbus. However, an understanding of the format will be very 
useful if you decide to build additional tooling.

The Lightbus schema format is simply a collection of child JSON Schemas.
Below is the schema for the example `auth` API shown above:

```json
// Auto-generated schema for auth API
{
  "auth": {
  
    // Events specify only parameters
    "events": {
      "user_registered": {
        "parameters": {
          "$schema": "http://json-schema.org/draft-04/schema#",
          "title": "Event auth.user_registered parameters",
          "type": "object",
            "username": {
              "type": "string"
            },
          "properties": {
            "email": {
              "type": "string"
            },
            "is_admin": {
              "default": false,
              "type": "boolean"
            }
          },
          "required": [
            "username",
            "email"
          ],
          "additionalProperties": false
        }
      }
    },
    
    // RPCs specify both parameters and response
    "rpcs": {
      "check_password": {
        "parameters": {
          "$schema": "http://json-schema.org/draft-04/schema#",
          "title": "RPC auth.check_password() parameters",
          "type": "object",
          "properties": {
            "username": {
              "type": "string"
            },
            "password": {
              "type": "string"
            }
          },
          "required": [
            "username",
            "password"
          ],
          "additionalProperties": false
        },
        
        "response": {
          "$schema": "http://json-schema.org/draft-04/schema#",
          "title": "RPC auth.check_password() response",
          "type": "boolean"
        }
      }
    }
  }
}
```
 
The generalised format is as follows:

```json
// Generalised Lightbus schema format
{
  "<api-name>": {
  
    "events": {
      "<event-name>": {
        "parameters":  { /* json schema */ }
      }
      // additional events
    },
    
    "rpcs": {
      "<rpc-name>": {
        "parameters": { /* json schema */ },
        "response": { /* json schema */ }
      }
      // additional procedures
    }
    
  }
  // additional APIs
}
```

!!! note
    Lightbus will likely upgrade to a newer JSON Schema version once the [jsonschema Python library] has the [requisite support].
    

[type hints]: https://docs.python.org/3/library/typing.html
[oneOf]: https://spacetelescope.github.io/understanding-json-schema/reference/combining.html#oneof  
[enum]: https://spacetelescope.github.io/understanding-json-schema/reference/generic.html#enumerated-values
[pattern properties]: https://spacetelescope.github.io/understanding-json-schema/reference/object.html#pattern-properties
[specific typed properties]: https://spacetelescope.github.io/understanding-json-schema/reference/object.html#properties
[maxItems/minItems]: https://spacetelescope.github.io/understanding-json-schema/reference/array.html#length
[items]: https://spacetelescope.github.io/understanding-json-schema/reference/array.html#tuple-validation
[jsonschema Python library]: https://github.com/Julian/jsonschema
[requisite support]: https://github.com/Julian/jsonschema/issues/337
