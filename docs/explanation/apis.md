# APIs

When we refer to an *API*, we are referring to an `Api` class definition.
**All functionality on the bus is defined using APIs.**

For example, consider an API for support tickets within a company's 
help desk:

```python3
class TicketApi(Api):
    ticket_created = Event(parameters=('id', 'sender', 'subject', 'body'))

    class Meta:
        name = 'help_desk.ticket'

    def get(self, id):
        return get_ticket_from_db(pk=id)
```

This API defines an event, a procedure, and the name used to address the API
on the bus. The help desk service could define multiple additional APIs as needed
(perhaps for listing help desk staff or retrieving reports).


## API registration & authoritative/non-authoritative APIs

An API can be registered with your service's bus client as follows:

```python3
import lightbus
from my_apis import HelpDeskApi

bus = lightbus.create()

# Register the API with your service's client
bus.client.register_api(HelpDeskApi())
```

Registering an API will:

1. Allow you to **fire events** on the API using the service's client
1. Cause the lightbus worker for this service (i.e. `lightbus run`) 
   to **respond to remote procedure calls** on the registered API

We say that a service which registers an API is *authoritative* for that API.
Services which do not register a given API are *non-authoritative* for the API.
**Both authoritative and non-authoritative services can listen for events on any API and 
call remote procedures on any API.**

For example, a separate online store service could not fire the `help_desk.ticket_created`
event on the API we defined above. Nor would you reasonably expect the online store to 
services remote procedure calls for `help_desk.ticket_created()`.

## Why?

Preventing the online store service from responding to **remote procedure calls** for the 
help desk service makes sense. There is no reason the online store should have any 
awareness of the help desk, so you would not expect it to respond to remote 
procedure calls regarding tickets. 

Therefore, the logic for allowing only authoritative services to respond to remote procedure calls
is hopefully compelling.

The case for limiting event firing to authoritative services is one of architecture, maintainability,
and consistency:

* Allowing any event to be fired by any service within your organisation could quickly 
  lead to spiraling complexity.
* The authoritative service will always have sufficient information to guarantee basic validity of an 
  emitted message (for example, the event exists, required parameters are present etc). As a result errors 
  can be caught earlier, rather than allowing them to propagate onto the bus and potentially impact distant
  services.

We welcome discussion on this topic, [open a GitHub issue] if you would like to discuss this further.

[services]: services.md
[open a GitHub issue]: https://github.com/adamcharnock/lightbus/issues
