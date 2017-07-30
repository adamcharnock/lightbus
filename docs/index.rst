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
    todo

.. readingtime::

*Lightbus - Filling the gap between monolithic and microservice (design stage)*

.. raw:: html

    <strong>

Lightbus will be a new :ref:`message bus <motivation:Task queue vs bus>`
for Python 3, :ref:`backed by AMQP <implementation/index:Why AMQP>`. Our
focus is providing conceptually simple communication between multiple
applications/processes.

.. raw:: html

    </strong>

Lightbus will be able to substitute for task queues such as Celery &
Rq, but it will encourage more extensible and loosely coupled
system design.

Lightbus will not be aimed at microservice architectures. Rather at
cases where several non-micro applications which require some level of
coordination. See :ref:`Lightbus positioning <alternatives:Lightbus positioning>` for further details.

Request for Comments
--------------------

I have created the design document to solicit feedback, critique, and general interest.
Additionally, I'm interested in thoughts regarding the :doc:`concerns section <concerns>`
section.

**How to get in touch:**

* Open a `new GitHub issue`_
* Email adam@[projectname].org
* Relevant Hacker News / Reddit threads

.. important::

    Please include some information regarding your projects' particular circumstances –
    team size, project size, current architecture etc. This
    helps give some context to any discussion.

Lightbus goals
--------------

-  RPC
-  Events (pub/sub)
-  Ease of development & debugging
-  Excellent tooling & documentation
-  Targeting smaller teams
-  High speed & low latency (but not at the expense of other goals)

What Lightbus is not
--------------------

We explicitly do not wish to support the following:

-  Microservice architectures
-  High volume (arbitrarily set at over 5,000 messages per second) [#f1]_

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
.. _new GitHub issue: https://github.com/adamcharnock/lightbus/issues/new

.. [#f1] There is no specific reason for this, except that the goal of Lightbus is
        not to achieve the extremes of throughput. Where that line lies is unclear,
        so for now I've chosen an arbitrary value.
