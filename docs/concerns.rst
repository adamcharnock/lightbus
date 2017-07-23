Concerns
========

.. readingtime::

History repeating
-----------------

Presumably this has all been done before.
What did/do those implementations look like? What were their failings? Am
I bound to repeat them? (Looking at you, `ESB`_)

AMQP suitability
----------------

Rabbit MQ, the most popular AMQP broker, does not handle network partitions
particularly well. Is it a concern? Would an alternative such as ActiveMQ be a suitable alternative?
Are there reasons AMQP protocol wouldn’t be suitable in general?

Demand
------

Is there demand for a project such as this? Do others encounter these
pain points? If not, why not?

`GitHub stars`_ will be taken as a vote for 'I want this'.

Collaborators
-------------

Currently it is just me, `Adam Charnock`_ (`GitHub`_). These things are more
sustainable with multiple people and I am therefore very interested in working on this with others.
More details below.

Niche
-----

Have I correctly identified that there is a niche which isn't being filled
by existing task queues and buses? Am I missing something obvious?

.. figure:: _static/images/boat.jpg
    :align: center
    :alt: Someone in a canoe on a canal. Not me.

    That's not me.  Next: :doc:`implementation/index`

.. _GitHub stars: https://github.com/adamcharnock/lightbus
.. _Adam Charnock: https://adamcharnock.com/
.. _GitHub: https://github.com/adamcharnock/
.. _ESB: https://en.wikipedia.org/wiki/Enterprise_service_bus
