Alternatives
============

.. readingtime::

.. contents::
    :local:
    :backlinks: none

Below I examine the current alternatives to Lightbus with the intention
of determining how Lightbus should be positioned. I've also included any
relevant highly voted/commented issues for each project as this may indicate
demand for additional features.

Celery
------

.. image:: _static/images/alternatives/celery.png
    :align: right
    :width: 100
    :alt: Celery logo

`Celery`_ is one of the most popular Python task queues. It provides
its own scheduler service, supports multiple backends, and can
provide both pub/sub & RPC.

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

Top voted Celery issues
~~~~~~~~~~~~~~~~~~~~~~~

* `Proposal to deprecate Redis as a broker support <https://github.com/celery/celery/issues/3274>`_
  – They kept it
* The vast majority of other top-voted issues are bugs

Rq
----

.. image:: _static/images/alternatives/rq.png
    :align: right
    :width: 100
    :alt: RQ logo

`Rq`_ is a simple Redis-backed task queue. It provides minimal functionality
but has a very simple API and clear documentation.

There is a separate `Rq Dashboard`_ project which provides a simple interface onto
the brokers current state.

Learnings from Rq
~~~~~~~~~~~~~~~~~

* A simple API with clear documentation goes a long way
* Provide additional functionality in separate projects (i.e. dashboard, scheduling)
* Jobs are defined by the function's import path. This will not scale well to multiple
  applications, and I believe will be too fragile for Lightbus' goals.

Top voted Rq issues
~~~~~~~~~~~~~~~~~~~

* `Enable automatic reload upon source code changes <https://github.com/nvie/rq/issues/2>`_
* `Worker concurrency <https://github.com/nvie/rq/issues/45>`_

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

Top voted Zato issues
~~~~~~~~~~~~~~~~~~~~~

No issues that are both relevant and highly voted/commented.

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
-  Could be space for improved debugging & developer tools

Learnings from Nameko
~~~~~~~~~~~~~~~~~~~~~

-  **Dependency injection** – I’m not sold on `DI`_ in Python. I get the impression that it is a
   rather verbose way of achieving `IoC`_ compared to what python supports
   natively as a dynamic language. This feature *may* also be more applicable to larger teams.
-  **Shell** – An excellent ideal. Perhaps via `bpython`_ ?
-  **Services** – I'm inclined to define APIs rather than Services. Lightbus is
   aimed at existing backend applications which need to communicate, rather than service-oriented architectures.
   This distinction may be as conceptual as it is practical.
-  **Tooling** – I'd like to enhance the available tooling
-  **Documentation** – I'd like to offer more detailed documentation, both API and narrative.
   However, this can be easier said than done. Planning and outlining will be important in
   order to do this well.

Top voted Nameko issues
~~~~~~~~~~~~~~~~~~~~~~~

* `Auto reload app with nameko run? <https://github.com/nameko/nameko/issues/420>`_

Lightbus positioning
--------------------

The following table considers what features different sized projects may
consider a ‘must have’ requirement. For the sake of this simple
analysis, a small project may be considered a hobby project or smaller
commercial project, likely with a sole developer. A large project would
be one with a team of 10+ developers serving significant traffic. A
medium project would be everything in between.

I expect few projects will fit neatly into one of the columns below.
However, my hope is that these broad strokes will be useful regardless.

**Lightbus will be targeting the 'medium' category.**

+------------------------------------------------+-----------------+------------------+-----------------+
| Feature                                        | Small project   | Medium project   | Large project   |
+================================================+=================+==================+=================+
| Support for simple (non-AMQP) brokers          | ✔               | -                | -               |
+------------------------------------------------+-----------------+------------------+-----------------+
| Can function on non-trusted network (Heroku)   | ✔               | ?                | -               |
+------------------------------------------------+-----------------+------------------+-----------------+
| Simple deployment & maintenance                | ✔               | ✔                | -               |
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
| Pub/sub                                        | -               | ✔                | ✔               |
+------------------------------------------------+-----------------+------------------+-----------------+
| Multiple broker support                        | -               | -                | ✔               |
+------------------------------------------------+-----------------+------------------+-----------------+
| Enterprisey features (Auditing, LDAP auth,     |                 |                  |                 |
| regulation, compliance...)                     | -               | -                | ✔               |
+------------------------------------------------+-----------------+------------------+-----------------+

.. _Nameko: https://github.com/nameko/nameko
.. _bpython: https://github.com/bpython/bpython
.. _sane default settings: https://library.launchkit.io/three-quick-tips-from-two-years-with-celery-c05ff9d7f9eb
.. _getting started guide: http://celery.readthedocs.io/en/latest/getting-started/index.html
.. _navigation: https://kubernetes.io/docs/home/
.. _terrifying: https://zato.io/docs/index.html
.. _Django: https://docs.djangoproject.com/
.. _DI: https://wikipedia.org/wiki/Dependency_injection
.. _IoC: https://wikipedia.org/wiki/Inversion_of_control
.. _Rq Dashboard: https://github.com/eoranged/rq-dashboard
.. _Rq: http://python-rq.org/

.. figure:: _static/images/rose.jpg
    :align: center
    :alt: Large painting of a rose, barely-functional piano in foreground

    I think my house is weird. Next: :doc:`concerns`

