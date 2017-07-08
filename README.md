# Design Document

*Warren - Filling the gap between monolithic and microservice*

Warren will be a new message queue for Python 3, backed by AMQP. 
The emphasis is on providing conceptually simple communication 
between multiple applications/processes.

This is provisionally not aimed at microservice architectures. Rather 
at cases where there are several non-micro applications which require some 
level of coordination.

*Warren* is the working name for the project, it ~~may~~ will change 
as the name is [already taken](https://pypi.python.org/pypi/warren/0.1.0).

**TL;DR:** Short of time? I’m particularly interested in responses to the ‘Concerns’ section below.

## Goals

* A backend RPC & pub/sub between applications
* Ease of development & debugging
* Excellent tooling & documentation
* Targeting smaller teams
* High speed & low latency (but not at the expense of other goals)

## Non-goals

We explicitly do not wish to support the following:

* Microservice architectures
* High volume (> 5,000 messages per second, at least initially)

## Assumptions

* APIs exposed on trusted network only

## Example use

A company has several Python-based web applications for handling 
sales, spare parts, support, and warranty registrations. These applications are 
separate entities, but need a backend communication system for coordination and data 
sharing. The support app checks the user has a valid warranty, the warranty app 
pulls data in from sales, and the support app also needs information regarding spare 
part availability.

Warren provides a uniform backend allowing these applications to expose their own 
APIs and consume the APIs of others. These APIs feature both methods to be called 
(RPC) and events which can be published & subscribed to (PUB/SUB).

## Motivation

*This was originally and briefly discussed in a 
[Hacker News thread](https://news.ycombinator.com/item?id=14556988).*

Current Python message queues function well in the case where there 
is a single app which needs to queue tasks for execution later.
However, they seem poorly suited to multi-application ecosystems.
The reasons for this can be broken down as follows:

**Broker limitations** - Queues such as [rq](http://python-rq.org/)
are limited by the use of Redis as a broker. This 
becomes a problem when dealing with loosely coupled apps (see ‘Why AMQP’).

**Complexity** - [Celery](http://celery.readthedocs.io/) in particular 
becomes particularly conceptually complex when dealing with with 
multiple applications communicating via AMQP. This is partly because 
Celery's terminology overlaps and somewhat conflicts with that of AMQP.
It is also because the Celery documentation is pretty light on details 
when it comes to more complex setups (as is Google).

**Conceptual mapping** - Messages sent via apps seem to break down into 
two categories, *events* & *calls*. Event messages should be sent without 
caring who is listening and without expecting a response. Additionally, an app should 
be able to have multiple listeners for an event. Calls 
require that a process is present to respond, and the response must be 
returned to the calling process. I believe surfacing this distinction 
in the message queue library would significantly simplify client code 
and reduce boilerplate.

**Testing & debugging** - I’ve found writing tests for existing 
queues to be difficult. I want simple ways to both assert that a message was 
created and simulate incoming messages. Both should take identical parameters.
I would also like to see much better debugging tools, to help answer the question 
“Why isn’t App B receiving message X from App A?”

## Why AMQP

For the reasons detailed above I am proposing that this message queue be 
tightly coupled to the underlying broker (i.e. because 
supporting multiple brokers leads to significant complexity in other popular message queues).

I am also proposing that the broker be AMQP-based 
(e.g. [RabbitMQ](https://www.rabbitmq.com)). This is because I believe 
AMQP provides the features needed to loosely couple applications via a message queue.

For example, I want to send a ``user.registered`` event from App A. App A should 
be able to send this without knowing if anyone is listening for it, without knowing 
what queue it should go on, and without knowing anything about the implementation
of any event handlers. Moreover, App B should be able to listen for ``user.registered`` without 
having to know anything about where the event comes from.

This isn’t possible with brokers such as Redis because App A needs push a message 
to the queue that App B is listening on. App A therefore needs to know that App B exists and 
that it is listening on a particular queue. Additionally, if App C then also wants to listen 
for the event then it will need its own queue. At this point App A needs to enqueue the message *twice*, 
once for App B and once for App C.

AMQP solves this by adding the concept of ‘exchanges’.
With AMQP, App B would create its own queue and configure it to receive messages 
from one or more exchanges, perhaps also filtering for only certain messages.
App A sends a message to the AMQP *exchange*. AMQP then places that message into 
each queue listening on that exchange. This includes the queue that App B created, 
and therefore App B receives the message.

Note that in this case App A only had to know what exchange to send the message to, 
and the logic for receiving messages lies entirely in the hands of the receivers.
App C and App D could come along and create their own queues and receive events 
without App A ever knowing or caring.

## Concerns

**History repeating** - Presumably this has all been done before. 
What did/do those implementations look like? What were their failings? Am 
I bound to repeat them? ([ESB](https://en.wikipedia.org/wiki/Enterprise_service_bus)?)

**AMQP suitability** - Rabbit MQ, the most popular AMQP broker, does not handle network paritions 
particularly well. Is it a concern? Would an alternative such as ActiveMQ be a suitable alternative? 
Are there reasons AMQP protocol wouldn’t be suitable in general?

**Demand** - Is there demand for a project such as this? Do others encounter these 
pain points? If not, why not?

**Collaborators** - Currently it is just me, @adamcharnock. These things are more 
sustainable with multiple people and I am therefore very interesting in workin on this with others. 
More details below.

**Microservices** - I am uncertain of the suitability of broker-based 
message transport for microservice architectures. In particular, I’ve seen 
latencies of around 300ms (via Celery) while waiting for a task to return a response.
This is fine for offline processes, but would probably but too slow for serving HTTP 
requests to users. Is it acceptable to discount this use case? 
Can these latencies be reduced? Could/should ZeroMQ be used to return the responses, 
thereby reducing latencies? **Update:** [Promising results with AMQP + ZeroMQ](https://github.com/adamcharnock/warren/tree/master/experiments/kombu_zmq).

### Nameko basically does this already

Yep, [nameko does this](http://nameko.readthedocs.io/). However:

* Nameko is aimed specifically at microservices
* Definition of APIs is very Service-oriented (this makes sense for microservices)
* Potential performance improvements to be gained from combining AMQP + ZeroMQ
* Space for targeting specifically non-microservices. Eg.
    * Syncing data models between applications
* Better debugging & developer tools

## Get involved!

I’d much prefer to work on this as a team. Input at the design stage will 
be particularly important, but coding and maintenance help is also excellent.

I’m hoping the implementation can be kept small and sleek, and I imagine this will 
be a slow burn over 12ish months.

There is probably also a web UI side-project down the road. Something for managing 
scheduled tasks, and perhaps monitoring/debugging.

## Implementation

Watch this space. I would like to at least partially address the above 
concerns before designing an implementation.

## Suggestions made

### Consider MQTT rather than AMQP

Questions to be answered:

* Does MQTT support loosely coupling processes as described above?
* What brokers are available?
* How do MQTT brokers compare to RabbitMQ et al? Stability, features, etc
* How easily can the brokers be deployed?

**Quotes of note:**

> We recommend the use of AMQP protocol to build reliable,scalable, and advanced clustering messaging infrastructuresover an ideal WLAN, and the use of MQTT protocol to supportconnections with edge nodes (simple sensors/actuators) underconstrained environments (low-speed wireless access) 

[source](http://sci-hub.io/10.1109/ccnc.2015.7158101)

### Consider Kafka rather than AMQP

Questions to be answered:

* Does Kafka support loosely coupling processes as described above?
* How easily can Kafka be deployed?
* **Update:** Kafka appears more difficult to setup. Plus we'd tie ourselves to 
  an implementation, rather than a protocol (in the case of AMQP)
