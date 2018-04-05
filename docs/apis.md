
!!! note
    We recommend read the [concepts](concepts.md) section before continuing
    as this will give you a useful overview before delving into the details 
    below.

APIs specify the functionality available on the bus. To do this you 
define API classes within (or import API classes into) your `bus.py` file.

For example:

```python3
class SupportCaseApi(Api):
    # An event
    case_created = Event(parameters=('id', 'sender', 'subject', 'body'))

    class Meta:
        # API name on the bus
        name = 'support.case'
    
    # A procedure
    def get(self, id):
        return get_case_from_db(pk=id)
```

A service can define zero or more APIs, and each API can contain 
zero or more events and zero or more procedures.

The `Meta` specifies options regarding the API, with `name` being 
the only required option. The name specifies how the API will be 
accessed on the bus.

You could use the above API as follows:

```python3
bus = lightbus.create()

# Call the get_case() RPC.
case = bus.support.case.get_case(id=123)

# Fire the case_created event
bus.support.case.case_created.fire(
    id=123,
    sender='Joe',
    subject='I need support please!',
    body='...',
)
```

## Options

### `name (str)`

Specifies the name of the API. This will determine how the API is addressed 
on the bus. See [naming](#naming), below.

`name` is a required option.

### `auto_register (bool) = True`

Should this API be registered with Lightbus upon import? This defaults to `True`, 
but if you specifically wish to prevent the API from being automatically 
registered with Lightbus you should set this to `False`.

## Naming

As you can see above, API names can contain periods to allow you 
to structure your bus in a suitable form for your situation. 
Some example API naming schemes may look like:

```coffeescript
Format:  <service>
Example: support.get_case()
         support.get_activity()


Format:  <service>.<object>
Example: support.case.get()
         support.activity.get()


Format:  <deparment>.<service>.<object>
Example: marketing.website.stats.get()
         ops.monitoring.servers.get_status()
```

## Authoritative and non-authoritative

The service which defines an API is *authoritative* for that API, and as 
such can perform some actions that are not allowed by services accessing the API.

A service which is authoritative for an API:

1. Must import the API class definition
2. Should respond to remote procedure calls for the API
   (i.e. by running a `lightbus run` process)
3. May fire events for the API

Conversely, a non-authoritative service may *not* perform the above actions. 
For example, the online store service could not fire the `bus.support.case.case_created`
event, nor should it import the `SupportCaseApi` class.

## Organising many APIs

* Will lightbus recognise a bus package as well as a bus module? 
  (i.e.`bus/__init__.py`?)


