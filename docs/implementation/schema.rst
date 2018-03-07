Schema
======

.. readingtime::

.. contents::
    :local:
    :backlinks: none


Lightbus will optionally support a schema format based upon `JSON Schema`_. A schema
file will specify information about one or more APIs. The values
of certain keys will be valid JSON schema definitions, and can therefore
be used to validate data.

Questions
---------

* Is there demand for a schema?
* Is JSON schema a good choice given Lightbus' :ref:`design goals <index:Lightbus goals>`?
* Is the Lightbus schema format sane? (hint: probably not yet)

Why use a schema
----------------

1. Tests can validate API use against a schema rather than requiring service to be present
2. Can be used to enhance tooling (e.g. `GraphQL interactive editor <http://graphql.org/swapi-graphql/>`_)
3. Can enhance reporting & monitoring

Our example API
---------------

We will be using our simple API from the :doc:`examples` page:

.. literalinclude:: code/auth_simple.py
    :name: provider

Generating a schema on your provider
------------------------------------

.. code-block:: python

    >> bus.dump_schema(file='./schema.json')

.. code-block:: python

    # ./schema.json
    {
        'my_company.auth': [
            'events': {
                'user_registered': {
                    'parameters': JSON_SCHEMA
                }
            },
            'rpcs': {
                'check_password': {
                    'parameters': JSON_SCHEMA,
                    'response': JSON_SCHEMA,
                }
            }
        ]
    }

This schema contains the basic information of the API.

Loading a schema on your consumer
---------------------------------

Loading a schema on the client is simple:

.. code-block:: python

    >>> bus = lightbus.create()
    >>> bus = bus.schema.load(file='./schema.json')

Error checking with schemas
---------------------------

Lightbus will – by default – validate all parameters and responses in cases where a schema is present.
This will be configurable for both arguments & responses. The intention of the
enabled-by-default state is to allow errors to be caught sooner.

You may wish to disable this in production for performance. However, Lightbus will still
be performant with validation enabled.

Manual validation will also be available as follows:

.. code-block:: python


    # Validate event dispatch arguments
    my_company.auth.user_registered.validate(
        parameters=dict(username='adam')
    )

    # Validate RPC call (can specify one or both of 'arguments' & 'response')
    my_company.auth.check_password.validate(
        parameters=dict(username='adam', password='secret'),
        response=True
    )


Specifying types
----------------

The schema we generated above contained basic API information, but not types. This
may or may not be acceptable to you. If you do wish to add types there are two methods
available:

* Python type hinting (easiest)
* Customising the schema files (most flexible)

Types using Python
~~~~~~~~~~~~~~~~~~

All we need to do us to update the above example using `Python type hinting`_
(available since Python 3.5):

.. literalinclude:: code/auth_types.py

Now we can generate our schema again and see the results:

.. code-block:: python

    >> bus.schema.dump(file='./schema.json')

.. code-block:: python

    # ./schema.json
    {
        'my_company.auth': [
            {
                'type': 'event',
                'name': 'user_registered',
                'arguments': {
                    'username': { 'type': 'string' }
                 }
            }, {
                'type': 'rpc',
                'name': 'check_password',
                'arguments': {
                    'username': { 'type': 'string' },
                    'password': { 'type': 'string' }
                },
                'response': { 'type': 'boolean' }
            }
        ]
    }

As you can see above, the schema now includes the following

* The available API names
* RPC & events available for each API
* Event parameters & types
* RPC parameters & types
* RPC return types

Multiple schema files
---------------------

Sometimes it may be preferable to store your schema in multiple files. For example:

* You will likely be consuming APIs from multiple producers. One schema per file obviates the need to merge schema files.
* You may prefer the readability of one API schema per file.

Dumping schemas to individual files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To dump multiple files, specify the ``directory`` argument to ``bus.schema.dump()``, rather than ``file``:

.. code-block:: python

    >>> bus.schema.dump(directory='./schema')

.. code-block:: shell


    $ ls ./schema
    my_company.auth.json
    my_company.customers.json
    my_company.sales.json

Loading schemas from individual files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    >>> bus = lightbus.create()
    >>> bus.schema.load(directory='./schema')

Generation of stub files
~~~~~~~~~~~~~~~~~~~~~~~~

I would like to support the generation of Python stub files to enable
IDE code completion:

.. code-block:: python

    >>> bus = lightbus.create()
    >>> bus.schema.load(directory='./schema')
    >>> bus.schema.generate_stubs('./.stubs')



.. _Python type hinting: https://docs.python.org/3/library/typing.html
.. _JSON Schema: http://json-schema.org/

Next
----

Next up: :doc:`choices` (the last section!)
