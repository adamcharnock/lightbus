Why AMQP (or not?)
==================

.. readingtime::

.. image:: /_static/images/rabbitmq.png
    :align: right
    :width: 100
    :alt: Rabbitmq logo

**Update October 2017:** I'm backing away from the idea of using AMQP.
I believe the requirements outlined below can be handled by consumer
groups brokers such as `Kafka`_ & `Redis with streams`_.

For the reasons detailed previously I am proposing Lightbus be
tightly coupled to the underlying broker (i.e. because
supporting multiple brokers leads to significant complexity in other popular message queues).

I am also proposing that the broker be AMQP-based
(e.g. `RabbitMQ`_, `ActiveMQ`_). This is because I believe
AMQP provides the features required to build a bus without requiring a custom broker.
In particular, it provides the decoupling of publishers and consumers.

For example, I want to send a ``user.registered`` event from App A. App A should
be able to send this event without knowing if anyone is listening for it, without knowing
what queue it should go on, and without knowing anything about the implementation
of any event handlers. Moreover, App B should be able to listen for ``user.registered`` without
having to know any specific details about where the event comes from.

This isn’t possible with brokers such as Redis because App A needs push a message
to the queue that App B is listening on. App A therefore needs to know that App B exists and
that it is listening on a particular queue. Additionally, if App C then also wants to listen
for the event then it will need its own queue. At this point App A needs to enqueue the message *twice*,
once for App B and once for App C.

AMQP solves this for us with the concept of ‘exchanges’.
With AMQP, App B would create its own queue and configure it to receive messages
from one or more exchanges, perhaps also filtering for only certain messages.
App A sends a message to the AMQP *exchange*. AMQP then places that message into
each queue listening on that exchange. This includes the queue that App B created,
and therefore App B receives the message.

Note that in this case App A only had to know what exchange to send the message to,
and the logic for receiving messages lies entirely in the hands of the receivers.
App C and App D could come along and create their own queues and receive events
without App A ever knowing or caring.

.. seealso:: :ref:`motivation:Task queue vs bus`

Next
----

Next up: :doc:`examples` (real made-up code!)

.. _RabbitMQ: https://www.rabbitmq.com
.. _ActiveMQ: http://activemq.apache.org/
.. _Kafka: https://kafka.apache.org/
.. _Redis with streams: http://antirez.com/news/114
