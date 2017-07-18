Alternatives
============

Celery / Rq
-----------

TBA

Zato.io
-------

TBA

TBA
----

-  Nameko is aimed specifically at microservices
-  Definition of APIs is very Service-oriented (this makes sense for
   microservices)
-  Potential performance improvements to be gained from combining AMQP +
   ZeroMQ
-  Space for targeting specifically non-microservices. Eg.

   -  Syncing data models between applications

-  Better debugging & developer tools

**What we could keep/ditch from the example provided by Nameko:**

-  Ditch: Dependency injection
-  I’m not sold on DI in Python. I get the impression that it is a
   rather verbose way of achieving IoC compared to what python supports
   natively as a dynamic language.
-  Keep: Shell
-  Change: Define APIs not services
-  This is also proving some clarity on how Lightbus would be different
   to Nameko. In this case it indicates that Lightbus has a bias towards
   inter-application communication, rather than being geared around
   microservices.
-  Enhance: Tooling
-  Enhance: Documentation

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


.. figure:: _static/images/rose.jpg
    :align: center
    :alt: Large painting of a rose, barely-functional piano in foreground

    I think my house is weird. Next: :doc:`concerns`
