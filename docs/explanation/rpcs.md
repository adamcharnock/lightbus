# Remote Procedure Calls (RPCs)

A remote procedure call is where you call a procedure available on the bus.
The sequence of events is:

* You call the RPC, `bus.auth.check_password()`
* An autoratitive process for the `auth` API handles the request and sends the response.
* You receive the result

---

Remote Procedure Calls are useful when:

1. You require information from a service [^1]
2. You wish to wait until a remote procedure has completed an action

You can perform an RPC as follows:

```python3
support_case = bus.support.case.get(pk=123)
```

RPCs do not provide a *fire and forget* mode of operation.
Consider using [events] if you need this feature.

## At most once semantics

TODO

## Considerations

TODO

* Conceptually simple
* Fragility
* Their use can be buried, leading to poor performance.
  Lightbus tries to alleviate this somewhat by using the
  `bus.api.method()` calling format, making it clear that this is a
  bus-based operation.

[^1]: This is also achievable with [events]. However you will need to listen
      for the events and likely store the data locally. See the [events]
      section for further discussion.


[events]: events.md
