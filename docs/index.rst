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
Rq, but it will encourage more a extensible and loosely coupled
system design.

Lightbus will not be aimed at microservice architectures (see below). Rather at
cases where several non-micro applications which require some level of
coordination.

Initial goals are as follows:

-  **Remote Procedure Call (RPC)** - Calling remote procedures and receiving the response
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

.. seealso::

    Further sample code can be found in :doc:`implementation/examples`,
    within the :doc:`implementation/index` section.

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
-  Lightbus will use an off-the-shelf AMQP broker rather than anything
   purpose-built (See also: :ref:`implementation/amqp:Why AMQP`).

.. note::

    Requiring private network would essentially prevent deployment on
    Heroku. This may be sufficient motivation to provide an alternative.

Fictional scenario
------------------

A company has several Python-based web applications for handling sales,
spare parts, support, and warranty registrations. These applications are
separate entities, but need a backend communication system for
coordination and data sharing. The support app checks the user has a
valid warranty, the warranty app pulls data in from sales, and the
support app also needs information regarding spare part availability.

Lightbus provides a uniform communication backend allowing these
applications to expose their own APIs and consume the APIs of others.
These APIs feature both callable methods (RPC) and events which can
be published & subscribed to (PUB/SUB).

Request for Comments
--------------------

I have created the design document to solicit feedback, critique, and general interest.
I am very open to comments on any aspect of the design.

**How to get in touch:**

* Open a `new GitHub issue`_
* Email adam@ *projectname* .org
* Relevant Hacker News / Reddit threads

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
