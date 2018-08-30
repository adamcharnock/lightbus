# Events

Firing an event will place the event onto the bus and return immediately. No information
is provided as to whether the event was processed, or indeed of it was received by any
other service at all. No return value is provided when firing an event.

This is useful when:

1. You wish to allow non-authoritative services to receive information without needing to concern yourself
   with their implementation
2. You wish the authoritative service to perform a known task in the background

The [quickstart](/tutorial/quick-start.md#events) provides an example of the latter case.

## At-least-once semantics

TODO

## Considerations

* More complex
* More robust

