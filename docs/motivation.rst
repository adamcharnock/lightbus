Motivation
==========

.. readingtime::

Current Python *task queues* (:ref:`Celery <alternatives:Celery>`, for example)
function well in the case where there is a
single app which needs to queue tasks for execution later. However, they
seem poorly suited to multi-application ecosystems.

Conversely, existing Python *message buses* (:ref:`Zato <alternatives:Zato>`, for example)
appear designed for much larger projects but at the cost of complexity
in both use and deployment.

Lightbus will target projects which fall in the gap between these two use
cases.

.. note::

    This was originally, briefly, and rantily mentioned in a `Hacker News thread`_.

Task queue vs bus
-----------------

A task queue typically enables a developer to execute a specific piece of code at a later date:

* A task queue is tightly coupled. The dispatching code must know what needs to be done.
* A task queue typically doesn't return results [#f1]_.

Buses on the other hand can be seen a little differently. A relevant definition is hard to pin
down, but I would say:

* A bus provides loose coupling. The dispatching code says what *did* happen, not what *should* happen.
* Additionally, a bus provides bi-directional communication.

Queue/bus example
~~~~~~~~~~~~~~~~~

A user registers with your application. They upload a profile image, and you also need to
send them a welcome email.

**Task queue:** Your signup code enqueues two tasks; one for the image resize, and one for the welcome email:

.. code-block:: python

    # TASK QUEUE

    # Handling the signup
    resize_profile_image.enqueue(user_info)
    send_welcome_email.enqueue(user_info)

    # Tasks executed later
    @task
    def resize_profile_image(user_info):
        pass

    @task
    def send_welcome_email(user_info):
        pass

**Bus:** Your signup code publishes a single event. This event describes what happened, rather than what should be done:

.. code-block:: python

    # BUS

    # Handling the signup
    user_signup.publish(user_info)

    # Tasks executed later
    @subscribe(user_signup)
    def resize_profile_image(user_info):
        pass

    @subscribe(user_signup)
    def send_welcome_email(user_info):
        pass

Benefits & downsides
~~~~~~~~~~~~~~~~~~~~

Benefits of this technique are:

* **More extensible** - events can be listened for without having to modify the producer
* **Future planning** - events can fired even if they are not immediately required
* **Less code** - marginal in the above example, but some events could have dozens of listeners
* **Loose coupling** is just pretty good 😀

Downsides are:

* **No longer explicit** - you cannot know what will happen by looking at the dispatch call [#f2]_.


.. figure:: _static/images/sunset.jpg
    :align: center
    :alt: Sunset with wind turbines.

    Nope, still not relevant. Don't get your hopes up. Next: :doc:`alternatives`


.. _Hacker News thread: https://news.ycombinator.com/item?id=14556988
.. _Zato: https://zato.io/
.. _rq: http://python-rq.org/
.. _Celery: http://celery.readthedocs.io/
.. _by Google: https://www.google.co.uk/search?q=define%3Abus

.. [#f1] Many tasks queus definitely do support receiving results.
         However, 1) the implementation often leaves something to be
         desired, and 2) the conceptual mapping feels odd.

.. [#f2] My hope is that tooling can help here.
