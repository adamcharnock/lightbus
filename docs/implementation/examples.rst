Example use
===========

.. readingtime::

Right, let's get down to code. Below are some thoughts on how the
Lightroom API could look. **This is a starting point for discussion
rather than anything particularly solid.**

Most of the following examples revolve around a simple API for
authentication.

Auth API example
----------------

Here we show a simple Lightbus user case. Note that:

* Lightbus auto-discovers your APIs in ``bus.py`` files (unless configured for manual registration)
* The client knows nothing of the API it is calling (see :doc:`schema <schema>`).

**Define the API**

Here we define a simple authentication API. This API has one method, ``check_password()``,
and one event, ``user_registered``.

.. literalinclude:: code/auth_simple.py
    :caption: Provider/server
    :name: provider

Note that we define an arbitrary API ``name`` of ``my_company.auth``. We will use this to
access the API below. You may use whatever naming scheme makes sense for your
particular situation. Names must be valid Python identifiers, separated by zero or more periods.

**Start the provider**

This starts the lightbus process. This will serve incoming requests.

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

**Consume the API**

You can now consume the API as follows:

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


This shows the simplest form of access. You can also export and load a schema
for you API. This will allow for validation of parameters at runtime.

I would also like to support the generation of Python stub files (``.pyi`` files).
This would allow for bus auto-completion with IDEs.

.. seealso::

    See the :doc:`schema` document for further details.


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
