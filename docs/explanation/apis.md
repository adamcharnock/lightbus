# APIs

When we refer to an *API*, we are referring to an `Api` class definition.
**All functionlaity on the bus is defined using APIs.**

For example, consider an API for support cases in the help desk service
mentioned in the [services] section:

```python3
class SupportCaseApi(Api):
    case_created = Event(parameters=('id', 'sender', 'subject', 'body'))

    class Meta:
        name = 'support.case'

    def get(self, id):
        return get_case_from_db(pk=id)
```

This API defines an event, a procedure, and the name used to address the API
on the bus. The help desk service could define multiple additional APIs as needed.

## Authoritative/non-authoritative APIs

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

[services]: services.md
