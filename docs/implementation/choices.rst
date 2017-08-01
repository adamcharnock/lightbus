Choices
=======

.. readingtime::

.. contents::
    :local:
    :backlinks: none

The following choices still need to be made

Python asyncio
--------------

Do we want to make use of asyncio? If so, how? Options are:

1. Provide entirely synchronous API
2. Provide entirely asynchronous API
3. Provide both – provide a synchronous wrapper for asynchronous API.

For example:

.. code-block:: python

    # Option 1 (synchronous)
    bus = lightbus.create()
    bus.auth.resize_avatar()

    # Option 2 (asynchronous)
    bus = lightbus.create(loop=...)
    await bus.auth.resize_avatar()

    # Option 3 (either)
    bus = lightbus.create(loop=...)
    bus.auth.resize_avatar()  # Synchronous
    await bus.auth.resize_avatar.async()  # Asynchronous alternative

I bias towards the simplicity of option 1 unless there is strong demand for this feature.

Differentiating time critical messages
--------------------------------------

Messages on the bus can be categorised based on their time criticality.
A time critical message is one upon which another process is blocking.

For example:

.. code-block:: python

    # Non time critical:
    bus.auth.resize_avatar()

    # Time critical:
    def my_view(request):
        full_name = bus.auth.get_full_name(request.username)
        return "Hello {}".format(full_name)

.. seealso::

    There is a strong argument that the non time critical use above should
    be implemented via an event. See also :ref:`implementation/choices:Allowing non-blocking RPC`.

Firstly, this will require two different API calls for the different cases. This could be:

.. code-block:: python

    # Blocking call:
    bus.auth.resize_avatar()

    # Non-blocking call (returns promise or similar):
    bus.auth.resize_avatar.delay()

It is also important to ensure time-critical messages are handled before
their non time-critical counterparts. The most natural choice for this would be to
use AMQP message priorities. We would need only two priority levels, 0 and 1. The former
would be regular low priority messages, the latter would be for time-critical messages.

.. note::

    Given the above, I wanted to ensure one could monitor queue length by individual priority.
    It appears it is possible to do this in RabbitMQ via the ``/api/queues`` API endpoint:

    .. figure:: /_static/images/rabbitmq-queue-length-paw.png
        :align: center
        :alt: Screenshot showing queue length by priority

        Showing queue statistics by priority from the ``/api/queues`` endpoint. This queue
        contains a single message of priority 3.

Provisional decision
~~~~~~~~~~~~~~~~~~~~

1. Differentiate time-critical messages via API use
2. Use message priorities to push time-critical messages to front of queue

Allowing non-blocking RPC
-------------------------

In :ref:`implementation/choices:Differentiating time critical messages` we suggest that
both the following forms of RPC call should be valid:

.. code-block:: python

    # Blocking call
    bus.auth.resize_avatar()

    # Non-blocking call
    bus.auth.resize_avatar.delay()


However, there is a strong argument that non-blocking RPCs should be entirely unsupported.
In these cases, events should be used. Using events promotes Lightbus' design goals of
loose coupling and extensibility.

However, a counterargument is that we are all responsible users [#f1]_ and sometimes
you just need to get stuff done.

Moreover, a non-blocking implementation will be required in order make multiple RPC
calls in parallel.

.. seealso:: :ref:`implementation/choices:Python asyncio`

Provisional decision
~~~~~~~~~~~~~~~~~~~~

1. Support async/non-blocking RPC
2. Documentation should include clear guidance promoting the use of events rather than non-blocking RPC

.. [#f1] Are we `no longer saying <https://github.com/kennethreitz/python-guide/issues/525>`_ 'consenting adults'?

Serialisation
-------------

How shall messages be serialised?

* Using JSON schema implies JSON
* However, `MessagePack`_ is JSON compatible so could be used also

Providing a pluggable serializer seems like a sensible choice. Defaulting to JSON
for readability and debugging would be wise.


Next
----

That's it, all done!


If you believe this idea is worth pursuing, please `star it on GitHub <https://github.com/adamcharnock/lightbus>`_
or :ref:`get in touch <index:Request for Comments>`.


.. _MessagePack: http://msgpack.org/
