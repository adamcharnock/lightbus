# Schema protocol (Redis)

Here we document the specific interactions between Lightbus and Redis. 
The concrete implementation of this is provided by the `RedisSchemaTransport` class.
Before reading you should be familiar with the [schema explanation].

This documentation may be useful when debugging, developing third-party client libraries, 
or simply for general interest and review. **You do not need to be aware of this 
protocol in order to use Lightbus**.

## Example API

We will use the following API within the examples in the remainder of this page:

```python3
from lightbus import Api, Event, Parameter

class AuthApi(Api):
    user_registered = Event(parameters=(
        Parameter('username', str),
        Parameter('email', str),
        Parameter('is_admin', bool, default=False),
    ))

    class Meta:
        name = 'my_company.auth'

    def check_password(self, username: str, password: str) -> bool:
        return username == 'admin' and password == 'secret'
```

## Schema format

The Lightbus schema format is a JSON structure containing multiple JSON schemas.
Below is the schema for the example `my_company.auth` API shown above 
(generalised format follows):

```json
// Auto-generated schema for auth API
{
  "my_company.auth": {

    // Events specify only parameters
    "events": {
      "user_registered": {
        "parameters": {
          "$schema": "http://json-schema.org/draft-07/schema#",
          "title": "Event my_company.auth.user_registered parameters",
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
          "$schema": "http://json-schema.org/draft-07/schema#",
          "title": "RPC my_company.auth.check_password() parameters",
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
          "$schema": "http://json-schema.org/draft-07/schema#",
          "title": "RPC my_company.auth.check_password() response",
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

## Per-schema Redis key

Each schema is stored in redis as a string-ified JSON blob using a key formed as follows:

```
schema:{api_name}
``` 

For example:

```
schema:my_company.auth
```

## Storing schemas

A schema can be stored on the bus as follows:

```
SET "schema:{api_name}" "{json_schema}"
SADD "schemas" "{api_name}"
EXPIRE "schema:{api_name}" "{json_schema}" {ttl_seconds}
```

Where:

* `{api_name}` is replaced with the fully qualified API name (e.g. `my_company.auth`)
* `{json_schema}` is replaced with the string-ified JSON structure detailed above
* `{ttl_seconds}` is replaced with the maximum time the schema should persist before being 
  expired. Schemas expire to ensure shutdown Lightbus processes no longer advertise their APIs.
  Lightbus has a default value of 60 seconds.

This process must be repeated at least every `ttl_seconds` in order to keep the schema active 
on the bus.

## Loading schemas

Available schemas are loaded as follows:

```
schemas = SMEMBERS "schemas"
for api_name in scheams:
    GET "schema:{api_name}"
```

A schema is only available if the `GET` succeeds. The data returned by the `GET` should be JSON decoded, 
and will contain the structure detailed above.


[schema explanation]: ../explanation/schema.md
