Schema
======

.. readingtime::

.. contents::
    :local:
    :backlinks: none


**Early design stage**

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

1.  Makes developing & testing consumers easier
2.  Can test output against schema
3.  Can test method use against schema

    .. code-block:: python

        # Validate parameters. In this case validation fails.
        AuthApi.check_password.validate(
            username='adam',
            bad_key='secret'
        )

4. Can validate fixtures against schema

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
            {
                'type': 'event',
                'name': 'user_registered',
                'arguments': ...,  # TBC
            }, {
                'type': 'rpc',
                'name': 'check_password',
                'arguments': {
                    'username': {},
                    'password': {}
                }
            }
        ]
    }

This schema contains the basic information of the API. However, types are notably missing
(more on this in :ref:`implementation/schema:Specifying types`)

Loading a schema on your consumer
---------------------------------

Loading a schema on the client is simple:

.. code-block:: python

    >>> bus = lightbus.create()
    >>> bus = lightbus.add_schema(file='./schema.json')

Error checking with schemas
---------------------------

.. todo::

    Write me


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

    >> bus.dump_schema(file='./schema.json')

.. code-block:: python

    # ./schema.json
    {
        'my_company.auth': [
            {
                'type': 'event',
                'name': 'user_registered',
                'arguments': ...,  # TBC
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
* Methods & events available for each API
* Method parameters
* Method return types
* Event parameters (TODO)

.. todo::

    How to specify event parameter types in Python?

Multiple schema files
---------------------

Sometimes it may be preferable to store your schema in multiple files. For example:

* You will likely be consuming APIs from multiple producers. One schema per file obviates the need to merge schema files.
* You may prefer the readability of one API schema per file.

Dumping
~~~~~~~

To dump multiple files, specify the ``directory`` argument to ``dump_schema()``, rather than ``file``:

.. code-block:: python

    >>> bus.dump_schema(directory='./schema')

.. code-block:: shell

    $ ls ./schema
    my_company.auth.json
    my_company.customers.json
    my_company.sales.json

Loading
~~~~~~~

.. code-block:: python

    >>> bus = lightbus.create()
    >>> bus = lightbus.add_schema(directory='./schema')


.. _Python type hinting: https://docs.python.org/3/library/typing.html
.. _JSON Schema: http://json-schema.org/
