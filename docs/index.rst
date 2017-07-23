Introduction
============

.. toctree::
    :maxdepth: 2
    :numbered:
    :titlesonly:
    :hidden:

    self
    motivation
    alternatives
    concerns
    implementation/index

.. readingtime::

*Lightbus - Filling the gap between monolithic and microservice (design stage)*

**Lightbus will be a new message bus for Python 3, backed by AMQP. Our
focus is providing conceptually simple communication between multiple
applications/processes.**

Lightbus will be able to substitute for message queues such as Celery &
Rq, but it will encourage a more extensible and loosely coupled
architecture.

Lightbus will not be aimed at microservice architectures. Rather at
cases where several non-micro applications which require some level of
coordination.

**TL;DR:** Short of time? I’m particularly interested in responses to
the :doc:`concerns section <concerns>`.

Goals
-----

-  RPC
-  Events (pub/sub)
-  Ease of development & debugging
-  Excellent tooling & documentation
-  Targeting smaller teams
-  High speed & low latency (but not at the expense of other goals)

Non-goals
---------

We explicitly do not wish to support the following:

-  Microservice architectures
-  High volume (arbitrarily set at over 5,000 messages per second)

Assumptions
-----------

-  APIs exposed on trusted network only

.. note::

    Requiring private network would essentially prevent deployment on
    Heroku. This may be sufficient motivation to provide an alternative.

Example use
-----------

A company has several Python-based web applications for handling sales,
spare parts, support, and warranty registrations. These applications are
separate entities, but need a backend communication system for
coordination and data sharing. The support app checks the user has a
valid warranty, the warranty app pulls data in from sales, and the
support app also needs information regarding spare part availability.

Lightbus provides a uniform communication backend allowing these
applications to expose their own APIs and consume the APIs of others.
These APIs feature both methods to be called (RPC) and events which can
be published & subscribed to (PUB/SUB).

.. figure:: /_static/images/tent-at-night.jpg
    :align: center
    :alt: A glowing tent at night in an orange orchard. Pretty, but not relevant.

    Images make things less boring. Next up: :doc:`motivation`

.. _RabbitMQ: https://www.rabbitmq.com
.. _Hacker News thread: https://news.ycombinator.com/item?id=14556988



