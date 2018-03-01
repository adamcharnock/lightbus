Lightbus
========

.. image:: https://travis-ci.org/adamcharnock/lightbus.svg?branch=master
    :target: https://travis-ci.org/adamcharnock/lightbus

.. image:: https://coveralls.io/repos/github/adamcharnock/lightbus/badge.svg?branch=master
    :target: https://coveralls.io/github/adamcharnock/lightbus?branch=master


**Lightbus will be a new message bus for Python 3. The
focus of Lightbus is providing conceptually simple communication between multiple
applications/processes.**

Lightbus will be able to substitute for message queues such as Celery &
Rq, but it will encourage a more extensible and loosely coupled
architecture.

Running tests
-------------

Lightbus must currently test against Redis unstable (in order to test the Redis
backends). Once streams make it into Redis stable this process will become easier.

Docker
~~~~~~

The docker image will automatically pull download and compile the redis
unstable branch::

    docker build -t lightbus-test -f tests/Dockerfile .
    docker run lightbus-test

Manually
~~~~~~~~

You can run tests outside of docker as follows::

    py.test --redis-server=/path/to/unstable/build/of/redis-server

`See lightbus.org`_
-------------------

.. _See lightbus.org: http://lightbus.org/
