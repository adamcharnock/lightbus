APIs specify the functionality available on the bus. To do this you
define API classes within your `bus.py` file. You can also define
your API elsewhere and import it into your `bus.py` file.

**For further discussion of APIs [see the concepts section](../explanation/apis.md).**

## An example API

```python3
# An example API. You can define this in your bus.py,
# or import into your bus.py file from elsewhere

class SupportCaseApi(Api):
    # An event,
    # available at bus.support.case.case_created
    case_created = Event(parameters=('id', 'sender', 'subject', 'body'))

    # Options for this API
    class Meta:
        # API name on the bus
        name = 'support.case'

    # Will be available as a remote procedure call at
    # bus.support.case.get()
    def get(self, id):
        return get_case_from_db(pk=id)
```

A service can define zero or more APIs, and each API can contain
zero or more events and zero or more procedures.

The `Meta` class specifies options regarding the API, with `name` being
the only required option. The name specifies how the API will be
accessed on the bus.

You could call an RPC on the above API as follows:

```python3
bus = lightbus.create()

# Call the get_case() RPC.
case = bus.support.case.get_case(id=123)
```

You can also fire an event on this API:

```python3
bus = lightbus.create()

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
on the bus. See [naming](#naming-your-apis), below.

`name` is a required option.

## Naming your APIs

As you can from the `Meta.name` option in the example above, API names
can contain periods which allow you
to structure your bus in a suitable form for your situation.
Some example API naming schemes may look like:

```coffeescript
# Example API naming schemes for use within Meta.name
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

## Organising many APIs

* Will lightbus recognise a bus package as well as a bus module?
  (i.e.`bus/__init__.py`?) TODO
    * It does now
