Introduction
============

.. readingtime::

.. toctree::
    :maxdepth: 1
    :numbered:
    :titlesonly:

    self
    motivation
    alternatives
    concerns
    implementation/index


*Lightbus - Filling the gap between monolithic and microservice (design stage)*

.. raw:: html

    <strong>

Lightbus will be a new :ref:`message bus <motivation:Task queue vs bus>`
for Python 3, :ref:`backed by AMQP <implementation/amqp:Why AMQP>`. Lightbus will
focus on providing conceptually simple communication between multiple
applications/processes.

.. raw:: html

    </strong>

Lightbus goals
--------------

Lightbus will be able to substitute for task queues such as Celery &
Rq, but it will encourage a more extensible and loosely coupled
system design.

Lightbus will not be aimed at microservice architectures (see below). Rather,
it will be aimed at situations where several non-micro applications require
some level of coordination.

Initial goals are as follows:

-  **Remote Procedure Call (RPC)** - Calling remote procedures and receiving returned data
-  **Events (pub/sub)** - Broadcasting events which other applications may subscribe to
-  **Ease of development & debugging**
-  **Excellent tooling & documentation**
-  **Targeting smaller teams**
-  **High speed & low latency** (but not at the expense of other goals)

Quick example
-------------

.. container:: row

    .. container:: col

        **Define your API**

        .. code-block:: python

            class AuthApi(Api):
                my_event = Event()

                class Meta:
                    name = 'example'

                def hello_world(self):
                    return 'Hello world'

    .. container:: col

        **Make calls and listen for events**

        .. code-block:: python

            >>> bus = lightbus.create()
            >>> bus.example.hello_world()
            'Hello world'
            >>> bus.example.my_event.listen(
            ...     callback
            ... )

Further sample code can be found in :doc:`implementation/examples`,
within the :doc:`implementation/index` section of this proposal.

Not microservices
-----------------

Lightbus is not aimed at microservice architectures. If you decompose your
applications into many (hundreds/thousands) of services then perhaps consider
:ref:`alternatives:Nameko`. Lightbus is aimed at medium-sized projects which need
a common communications system for their backend applications.

See also, :doc:`alternatives` and :ref:`alternatives:Lightbus positioning`.

Assumptions
-----------

-  APIs exposed on a trusted network only
-  Lightbus will use an off-the-shelf broker rather than anything
   purpose-built (See also: :ref:`implementation/amqp:Why AMQP`).

.. note::

    Requiring private network would essentially prevent deployment on
    Heroku. This may be sufficient motivation to revise this decision.

Fictional scenario
------------------

A company has several Python-based web applications for handling sales,
spare parts, support, and warranty registrations. These applications are
separate entities, but need a backend communication system for
coordination and data sharing. The support app checks the user has a
valid warranty, the warranty app pulls data in from sales, and the
support app also needs information regarding spare part availability.

These applications also need to queue tasks for execution by background workers,
as well as execute tasks on a schedule.

Lightbus will provide a uniform communication backend allowing these
applications to expose their own APIs and consume the APIs of others.
These APIs feature both callable methods (RPC) and events which can
be published & subscribed to (pub/sub).

Lightbus will also be able to fill the role of a
:ref:`task queue <motivation:Task queue vs bus>`, as
well as provide a scheduling mechanism.

Request for Comments
--------------------

I have created the design document to solicit feedback, critique, and general interest.
I am very open to comments on any aspect of the design.

**How to get in touch:**

* Open a `new GitHub issue`_
* Email adam@ *projectname* .org
* Any relevant `Hacker News <https://news.ycombinator.com/item?id=14900465>`_ /
  `Reddit <https://www.reddit.com/r/Python/comments/6qwg9p/lightbus_proposal_for_a_new_python_message_bus/>`_ threads

.. important::

    Please include some information regarding your projects' particular circumstances –
    team size, project size, current architecture etc. This
    helps give some context to any discussion.

Next
----

Next up: :doc:`motivation`

.. _RabbitMQ: https://www.rabbitmq.com
.. _Hacker News thread: https://news.ycombinator.com/item?id=14556988
.. _new GitHub issue: https://github.com/adamcharnock/lightbus/issues/new
