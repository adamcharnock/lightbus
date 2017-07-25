Choices
=======

.. readingtime::

The following choices still need to be made

Python asyncio
--------------

Do we want to make use of asyncio? If so, how? Options are:

1. Provide entirely asynchronous API
2. Provide entirely synchronous API
3. Provide both – provide a synchronous wrapper for asynchronous API.

For example:

.. code-block:: python

    # Option 1
    bus = lightbus.create()
    bus.auth.resize_avatar()

    # Option 2
    bus = lightbus.create(loop=...)
    await bus.auth.resize_avatar()

    # Option 3
    bus = lightbus.create(loop=...)
    bus.auth.resize_avatar()  # Blocking
    await bus.auth.resize_avatar.async()  # Async alternative

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

Firstly, this will require two different API calls for the different cases. This could be:

.. code-block:: python

    # Blocking call:
    bus.auth.resize_avatar()

    # Non-blocking call (returns promise or similar):
    bus.auth.resize_avatar.delay()

It is also important to ensure time-critical messages are handled before
their non time-critical counterparts. This most natural choice for this would be to
use AMQP message priorities. We could need only two priority levels, 0 and 1. The former
would be regular low priority messages, the latter would be for time-critical messages.

