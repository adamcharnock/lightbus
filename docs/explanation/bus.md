# The bus

The bus is the communications channel which links all your
services together. Currently this is Redis.

You use `lightbus.create()` in your `bus.py` file to access
this bus:

```python3
# bus.py

import lightbus

bus = lightbus.create()
```

This creates a high-level client through which you can
call [RPCs] and fire [events].

## About buses

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

[RPCs]: rpcs.md
[events]: events.md
