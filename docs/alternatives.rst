Alternatives
============

Celery
------

.. image:: _static/images/alternatives/celery.png
    :align: right
    :width: 100
    :alt: Celery logo

`Celery`_ is one of the most popular Python task queues. It provides
its own scheduler service, supports multiple backends, and can
provide both pub/sub & rpc.

I've already touched on Celery in in :ref:`motivation:Existing task queues`.

Learnings from Celery
~~~~~~~~~~~~~~~~~~~~~

- **Complexity** – Celery can be complex to setup. Sensible defaults and
  simplifying concepts will be important.
- **Tight coupling** – See :ref:`motivation:Task queue vs bus`
- **Conceptual overlap** – The mapping between Celery concepts and AMQP concepts is
  both unclear and overlapping.
- **Sane defaults** – Taking care over `sane default settings`_ is important
- **Feeling** – Celery *feels* heavy. I like things that feel light.
- **Documentation organisation** – Celery has a lot of documentation, but I find it
  hard to navigate. Ideas: Separate product information from documentation.
  Have `navigation`_. Less scary `getting started guide`_.

Rq
----

.. image:: _static/images/alternatives/rq.png
    :align: right
    :width: 100
    :alt: RQ logo

TBA

Zato
----

.. image:: _static/images/alternatives/zato.png
    :align: right
    :width: 100
    :alt: Zato logo

Like Lightbus, `Zato`_ pitches itself as a bus (in this case, an Enterprise
Service Bus). It is actually a broker in itself, and features its own web UI.

Learnings from Zato
~~~~~~~~~~~~~~~~~~~

- **Documentation Friendliness** – The docs look `terrifying`_. Also, letter soup.
  Interestingly, `Django`_ has a similar layout but looks much more approachable.
- **Target audience** – My intuition says Zato is aimed at large teams/companies.
- **Screenshots** – I like images & screenshots. Let's make them pretty and readable.
- Perhaps if I manage to read the docs I'll add some technical observations.


Nameko
------

.. image:: _static/images/alternatives/nameko.png
    :align: right
    :width: 100
    :alt: Nameko logo

`Nameko`_ is a popular framework aimed at microservices, supporting both
pub/sub and RPC. It is close to Lightbus in terms of features, but I think the
focus on microservices sets it apart.

-  Nameko is aimed specifically at microservices
-  Definition of APIs is very Service-oriented (this makes sense for
   microservices)
-  Potential performance improvements to be gained from combining AMQP +
   ZeroMQ
-  Could be spare for better debugging & developer tools

Learnings from Nameko
~~~~~~~~~~~~~~~~~~~~~

-  **Dependency injection** – I’m not sold on DI in Python. I get the impression that it is a
   rather verbose way of achieving IoC compared to what python supports
   natively as a dynamic language. This feature *may* also be more applicable to larger teams.
-  **Shell** – Definitely. Perhaps via `bpython`_ ?
-  **Services** – I'm inclined to define APIs rather than Services. Lightbus is
   aimed at existing apps which need to communicate, not stand-alone services.
   This distinction may be as conceptual as it is practical.
-  **Tooling** – I'd like to enhance to available tooling
-  **Documentation** – I'd like to offer more detailed documentation, both API and narrative.
   However, this can be easier said than done. Planning and outlining will be important in
   order to do this well.

Lightbus positioning
--------------------

The following table considers what features different sized projects may
consider a ‘must have’ requirement. For the sake of this simple
analysis, a small project may be considered a hobby project or smaller
commercial project, likely with a sole developer. A large project would
be one with a team of 10+ developers serving significant traffic. A
medium project would be everything in between.

Lightbus will be targeting the 'medium' category.

+------------------------------------------------+-----------------+------------------+-----------------+
| Feature                                        | Small project   | Medium project   | Large project   |
+================================================+=================+==================+=================+
| Support for simple (non-AMQP) brokers          | ✔               | -                | -               |
+------------------------------------------------+-----------------+------------------+-----------------+
| Can function on non-trusted network (Heroku)   | ✔               | -                | -               |
+------------------------------------------------+-----------------+------------------+-----------------+
| Conceptually simple                            | ✔               | ✔                | -               |
+------------------------------------------------+-----------------+------------------+-----------------+
| Scheduling                                     | ✔               | ✔                | ✔               |
+------------------------------------------------+-----------------+------------------+-----------------+
| Monitoring                                     | ?               | ✔                | ✔               |
+------------------------------------------------+-----------------+------------------+-----------------+
| Rate limiting                                  | -               | ✔                | ✔               |
+------------------------------------------------+-----------------+------------------+-----------------+
| Workflows (eg. task chaining)                  | -               | ✔                | ✔               |
+------------------------------------------------+-----------------+------------------+-----------------+
| Cross-project communication                    | -               | ✔                | ✔               |
+------------------------------------------------+-----------------+------------------+-----------------+
| RPC                                            | -               | ✔                | ✔               |
+------------------------------------------------+-----------------+------------------+-----------------+
| Pub/sub                                        | -               | ✔                |                 |
+------------------------------------------------+-----------------+------------------+-----------------+

.. _Nameko: https://github.com/nameko/nameko
.. _bpython: https://github.com/bpython/bpython
.. _sane default settings: https://library.launchkit.io/three-quick-tips-from-two-years-with-celery-c05ff9d7f9eb
.. _getting started guide: http://celery.readthedocs.io/en/latest/getting-started/index.html
.. _navigation: https://kubernetes.io/docs/home/
.. _terrifying: https://zato.io/docs/index.html
.. _Django: https://docs.djangoproject.com/

.. figure:: _static/images/rose.jpg
    :align: center
    :alt: Large painting of a rose, barely-functional piano in foreground

    I think my house is weird. Next: :doc:`concerns`

