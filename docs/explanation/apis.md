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

[services]: services.md
