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

Remote procedure calls will be processed at most once. In some situations the call will 
never be processed, in which case the client will raise a `LigutbusTimeout` exception. 

## Considerations

Whether to use RPCs or events for communication will depend upon your project's particular needs.
Some considerations are:

* RPCs are **conceptually simple**. You call a procedure and wait for a response. You do not need to 
  store any state locally, you can simply request data on demand (performance considerations aside).
* RPCs can be **fragile**. Any errors in the remote service will propagate to the client's service.
  You should handle these if possible.
* Their use within a codebase may be non-obvious, leading to poor performance.
  Lightbus tries to alleviate this somewhat by using the
  `bus.api.method()` calling format, making it clear that this is a
  bus-based operation.

[^1]: This is also achievable with [events]. However you will need to listen
      for the events and likely store the data locally. See the [events]
      section for further discussion.


[events]: events.md
