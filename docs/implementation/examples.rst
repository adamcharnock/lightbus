Example use
===========

.. readingtime::

Right, let's get down to code. Below are some thoughts on how the
Lightroom API could look. This is a starting point for discussion
rather than anything particularly solid.

Most of the following examples revolve around a simple API for
authentication.

Auth API example
----------------

Here we show a simple Lightbus user case. Note that:

* Lightbus auto-discovers your APIs in ``bus.py`` files (unless configured for manual registration)
* The client knows nothing of the API it is calling (see :doc:`schema <schema>`).


.. literalinclude:: code/auth_simple.py
    :caption: Provider/server
    :name: provider

You run the provider as follows:

.. code-block:: text
    :emphasize-lines: 1
    :caption: Running the provider/server
    :name: run

    $ lightbus start
    Lightbus provider starting...
    Found 1 API:
        - my_company.auth (1 method, 1 event)
    Connected to broker on amqp://rabbitmq
    Waiting for messages...

You will then be able to consume the API in another terminal window.

.. code-block:: python
    :caption: Consuming the API via a Python shell
    :name: consume

    # Create a simple schemaless bus
    >>> bus = lightbus.create()

    # Call the check_password() RPC method
    >>> bus.my_company.auth.check_password(
    ...     username='admin',
    ...     password='secret'
    ... )
    True

    # You can also listen for events...
    >>> bus.my_company.auth.user_registered.listen(my_callable)


In the above example we use ``my_company.auth`` as the API name. You are
free to shorten this to ``auth`` if you prefer, or even lengthen it to
``my_company.some_product.auth``.

Client error cases
------------------

.. code-block:: python
    :caption: Errors one may receive when making API calls.
    :name: errors

    # Method does not exist
    >>> bus.my_company.auth.foo()
    MethodNotFoundResponse: API 'my_company.auth' responded but
                            has no knowledge of method 'foo'

    # API does not exist
    >>> bus.my_company.foo.bar()
    UnresponsiveApi: Received no response from
                     'my_company.foo' within 1000ms


Using a schema will provide an additional layer of error checking prior to
attempting the API call. See :doc:`schema <schema>` for further details.
