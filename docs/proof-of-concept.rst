Proof of Concept
================

.. readingtime::

.. contents::
    :local:
    :backlinks: none

Work on a `proof-of-concept`_ is in progress. The design roughly matches that
proposed in the :ref:`implementation/index:Implementation` section.

The codebase is split as follows:

* Transports
* Declarative API syntax
* ``BusNode``
* ``BusClient``
* Message serialisation & deserialisation

Transports
----------

Transports provide plugable support for different message queues and message transports. This is therefore a
departure from the original intention to tightly tie the implementation directly to AMQP.

Three different types of transports are used:

* **RPC transports** – Handles the calling of RPCs, and the consuming of incoming RPCs
* **Result transports** – Conveys results of RPCs back to the caller
* **Event transports** – Handles consuming and listening for events

So far we have the following transports implemented:

====================  ===  ======  =====  ===========================================
Transport type:       RPC  Result  Event  Description
====================  ===  ======  =====  ===========================================
Debug                 ✔    ✔       ✔      Prints log messages only
Direct                ✔    ✔       ✔      Handles events & RPCs locally & directly
Redis                 ✔    ✔       ✔      Working transport using `Redis streams`_.
ZeroMQ                TBA  TBA     x      Will return results via ZeroMQ
====================  ===  ======  =====  ===========================================

It is also possible to mix & match the above transports. One could use the Redis transport for
RPCs & events, while returning the messages via ZeroMQ.

Declarative API syntax
----------------------

APIs are defined declaratively as follows:

.. code-block:: python3

    class AuthApi(Api):
        user_registered = Event(arguments=['username'])

        class Meta:
            name = 'auth'

        def check_password(self, username, password):
            return username == 'admin' and password == 'secret'

BusNode
-------

The ``BusNode`` class provides a developer-friendly interface for making use of the bus.
For example:

.. code-block:: python3

    import lightbus
    bus = lightbus.create()

    ### RPCs ###

    # blocking
    is_valid = bus.auth.check_password(
        username='user', password='password1'
    )

    # asynchronous
    is_valid = await bus.auth.check_password.call_async(
        username='user', password='password1'
    )

    ### Events ###

    def callback_fn(username):
        print(username + " registered!")

    # firing & listening
    bus.auth.user_registered.fire(username='user')
    bus.auth.user_registered.listen(callback_fn)

    # firing & listening (asynchronous)
    await bus.auth.user_registered.fire_async(username='user')
    await bus.auth.user_registered.listen_async(callback_fn)


BusClient
---------

The ``BusClient`` class wires together the ``BusNodes`` and the transports, as well as
handling message serialisation.

Message serialisation & deserialisation
---------------------------------------

Message serialisation is handled through the ``RpcMessage``, ``ResultMessage``, and ``EventMessage``
classes. Each class provides ``to_dict()`` and ``from_dict()`` methods which
can then be encoded by each transport in whatever way the transport deems fit.

Tasks remaining
---------------

See the `proof-of-concept`_ issue for an up-to-date list. However, a rough list follows (some
of which may not be included in the proof-of-concept:

* Schemas
* Plugable serialisation – we currently default to JSON serialisation
* Interactive REPL
* Debugging/monitoring interface – this will likely need to interact with the transports


.. _proof-of-concept: https://github.com/adamcharnock/lightbus/issues/1
.. _Redis streams: https://github.com/antirez/redis/tree/streams
