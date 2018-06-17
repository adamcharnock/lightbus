Lightbus allows processes to communicate through a
central medium, and has out-of-the-box support for Redis.
This can be done using any combination of RPCs or events.

We'll describe these concepts and their relationships in
more detail below.

## Service

Throughout this documentation we use the term *service*
to refer to one or more processes handling a common task.
These processes operate as a tightly-coupled whole.

For example, your company may run a help desk
(e.g `support.company.com`) , an online store (`store.company.com`),
and an internal image resizing service.
This documentation considers each of these to be a service.

The support and store services would both likely have a web process and a
Lightbus process each. The image resizing service would likely have a Lightbus
process only.

All processes in a service will generally:

* Share the same API class definitions
* Moreover, they will normally share the same codebase
* Create a single instance of the bus client in `bus.py` using
  `bus = lightbus.create()`.

A simple lightbus deployment could look something like this:

![A simple Lightbus deployment][simple-processes]

## Lightbus processes

A Lightbus process is started with the `lightbus run` command.
A Lightbus process typically waits for any bus activity
and responds accordingly.

Lightbus processes are therefore concerned with:

* Listening for events
* Responding to remote procedure calls

In handling either of these, the Lightbus process may fire
additional events or call other RPCs.

A service may have zero-or-more Lightbus processes. **A service
will only need a Lightbus process if it wishes to listen
for events or provide any RPCs which can be called.**

## Non-Lightbus processes

A web server would be a typical example of a non-Lightbus process
(e.g. Flask or Django). These processes can still interact with the
bus, but will typically do so briefly as part of their other duties.

Non-Lightbus process are therefore concerned with:

* Firing events
* Calling RPCs

## Bus

In computing, a bus is a shared communication medium. A bus allows any
software/hardware connected to that medium to communicate, as long as
common rules are obeyed. In this sense a bus is very similar to a conversation
between a group of people.

In electronics the communication medium can be a simple
copper cable. In software the communication medium is itself defined
by software.

**Lightbus uses Redis as its communication medium**, although support
for other mediums may be added in future.

!!! note

    The core of Lightbus mostly consists of a programming interface
    and presentational nicities.
    The connection with the communication medium is provided by
    customisable transports (see below). Lightbus ships with transports for Redis,
    but transports could be created for other mediums should you wish.

## API

When we refer to an *API*, we are referring to an `Api` class definition.
**All functionlaity on the bus is defined using APIs.**

For example, consider an API for support cases in the help desk service
mentioned above:

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

## Remote Procedure Calls (RPCs)

A remote procedure call is where you call a procedure available on the bus. The authoritative
service executes the procedure and you receive the result. This is useful when:

1. You require information from a service
2. You wish to wait until a remote procedure has completed an action

RPCs do not currently feature a 'fire and forget' mode of operation.

You can perform an RPC as follows:

```python3
support_case = bus.support.case.get(pk=123)
```

**RPCs provide at-most-once semantics.**

## Events

Firing an event will place the event onto the bus and return immediately. No information
is provided as to whether the event was processed, or indeed of it was received by any
other service at all. No return value is provided when firing an event.

This is useful when:

1. You wish to allow non-authoritative services to receive information without needing to concern yourself
   with their implementation
2. You wish the authoritative service to perform a known task in the background

The [quickstart](quick-start.md#events) provides an example of the latter case.

**Events provide at-least-once semantics.**

## Transports

Transports provide the communications system for Lightbus. There are four types
of transport:

* **RPC transports** – sends and consumes RPC calls
* **Result transports** – sends and receives RPC results
* **Event transports** – sends and consumes events
* **Schema transports** – stores and retrieves the [bus schema](schema.md)

Lightbus ships with a Redis-backed implementation of each of these transports.

Lightbus can be configured to use custom transports either globally, or on a per-API level.

[simple-processes]: static/images/simple-processes.png
