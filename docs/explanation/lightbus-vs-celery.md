# Lightbus vs Celery

Lightbus was conceived as a result of using Celery to communicate
between multiple Python services.

## Differences in principle

Celery is a task queue:

* A task queue is tightly coupled. The dispatching code must know what needs to be done
* A task queue typically doesn't return results

Lightbus is a bus:

* A bus provides loose coupling. The dispatching code says what did happen, not what should happen (events)
* A bus provides bi-directional communication (remote procedure calls)

## Differences in practice

A number of pain points were identified with Celery that Lightbus
aims to address. In particular:

* Single vs multi-[service] – Celery is targeted as being a task queue for a service, rather than a means for multiple services to interact.
* Conceptual overlap – The mapping between concepts in Celery and the underlying broker (AMQP at the time) is both unclear and overlapping.
  Lightbus provides a limited set of well defined concepts to avoid this confusion.
* Non-sane defaults – Some Celery settings have non-sane defaults, making setup somewhat perilous at times.
  Lightbus provides sane defaults for most circumstances, and documentation specifically geared to certain use cases ([metrics], [event sourcing])
* Tight coupling (as discussed above) – Celery tasks define the action to take, not what happened. Lightbus uses events,
  which describe happened, and listening services decide the action to take.
* General feeling – Celery feels large and opaque, debugging issues was challenging. Lightbus aims to feel lightweight, with clear
  logging and debugging tools.

[service]: ../explanation/services.md
[metrics]: ../howto/metrics.md
[event sourcing]: ../howto/event-sourcing.md
