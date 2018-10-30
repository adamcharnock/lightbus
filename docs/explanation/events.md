# Events

Firing an event will place the event onto the bus and return immediately. No information
is provided as to whether the event was processed, or indeed of it was received by any
other service at all. No return value is provided when firing an event.

This is useful when:

1. You wish to allow non-[authoritative] services to receive information without needing to concern yourself
   with their implementation
2. You wish the authoritative service to perform a known task in the background

The [quickstart](/tutorial/quick-start.md#events) provides an example of the latter case.

## At-least-once semantics

Delivering a message exactly once is Very Difficult.
Delivering a message at-most-once, or at-least-once is
much more practical. **Lightbus therefore provides
at-least-once delivery for events**.

As a result you can assume your event listeners will
always receive an event, but sometimes a listener may
be called multiple times for the same event.

You can handle this by ensuring your event listeners
are idempotent. That is, implement your event listeners in such a
way that it doesn't matter how many times they are executed.

See [how to write idempotent event handlers].

## Service names & listener names

An event will be delivered once to each *consumer group*. A consumer
group is identified by a name in the form:

    # Consumer group naming format
    {service_name}-{listener_name}

Your *service name* is specified in your [service-level configuration].
Your *listener name* is setup when you create your event listener (see below).

For example, this `bus` module sets up two listeners. Each listener is
given a `listener_name`, thereby ensuring each listener receives a
copy of every `competitor_prices.changed` event.

```python3
from my_handlers import send_price_alerts, update_db

bus = lightbus.create(
    service_name='price-monitor',
)

# Consumer group name: price-monitor-send-price-alerts
bus.competitor_prices.changed.listen(
    send_price_alerts,
    listener_name="send_price_alerts",
)

# Consumer group name: price-monitor-update-db
bus.competitor_prices.changed.listen(
    update_db,
    listener_name="update_db",
)
```

!!! important

    Failure to specify `listener_name` in the above example will
    result in each message going to **either** one listener or the other,
    but never to both. This is almost certainly not what you want.

## Process names

Your [service-level configuration] also specifies a *process name*.
This is less critical than the service name & listener name pairing
described above, but still serves an important function.

The *process name* is used to track which process within a consumer group
is dealing with a given event.

This is primarily useful when a service runs multiple Lightbus
processes, normally as a result of scaling or reliability requirements.
The purpose of a *process name* is twofold:

1. Each process has a per-determined length of time to handle and
   acknowledge a given event. If this time is exceeded then
   failure will be assumed, and the event may be picked up by another process.
2. When a Lightbus process starts up it will check for any
   outstanding events reserved for its process name. In which case it
   will process these messages first. This can happen if the process was
   killed prior to acknowledging messages it was processing.

Providing there is no timeout, **an event will only be delivered
to one process per listener within a service**.

The default process name is a random 4 character string. If left unchanged,
then clause 2 (above) will never be triggered (as a process name will not
persist between process restarts). Any outstanding messages
will always have to wait for the timeout period to expire, at which point
the will be picked up by another process.

## Considerations

* Events are more complex, you may need maintain state as events are received. 
  The source-of-truth regarding stored state may no longer be clear. Enforcing 
  consistency can become difficult.
* Events are more robust. Your service will be able to fire events as long as the bus 
  client can connect. Likewise, you service can listen for events until the cows come home.
  Incoming events may be delayed by problems in other services, but each service should 
  be isolated from those problems.
  
Concepts such as Domain Driven Design and Event Sourcing can help to tackle some 
of these problems.

## Best practices

You may find some of these best practices & suggestions useful. Just 
remember that there can be exceptions to every rule.

!!! note

    See [architecture tips](architecture-tips.md) for further details.

### Event naming

* Name events using the past tense. Use `page_viewed`, not `page_view`. 
  Use `item_created`, not `create_item`.
* Where relevant, consider using domain-based naming rather than technical names.
  For example, use `order_placed`, not `order_created`. Use 
  `parcel_delivered`, not `parcel_updated`.

### Parameter values

* Consistent meaning over time (not 'tomorrow', or '6 days ago')
* Chop up your relations (aggregates in DDD)


[service-level configuration]: /reference/configuration.md#2-service-level-configuration
[how to write idempotent event handlers]: /howto/write-idempotent-event-handlers.md
[authoritative]: /explanation/apis.md#authoritativenon-authoritative-apis
