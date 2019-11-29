
What is Lightbus?
=================


.. image:: https://img.shields.io/circleci/build/github/adamcharnock/lightbus
   :target: https://circleci.com/gh/adamcharnock/lightbus/tree/master
   :alt: CircleCI


.. image:: https://api.codacy.com/project/badge/Grade/801d031fd2714b4f9c643182f1fbbd0b
   :target: https://www.codacy.com/app/adamcharnock/lightbus?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=adamcharnock/lightbus&amp;utm_campaign=Badge_Grade
   :alt: Codacy Badge


.. image:: https://api.codacy.com/project/badge/Coverage/801d031fd2714b4f9c643182f1fbbd0b
   :target: https://www.codacy.com/app/adamcharnock/lightbus?utm_source=github.com&utm_medium=referral&utm_content=adamcharnock/lightbus&utm_campaign=Badge_Coverage
   :alt: Codacy Badge


.. image:: https://img.shields.io/discord/645218336229031946
   :target: https://discord.gg/2j594ws
   :alt: Discord


.. image:: https://img.shields.io/badge/Contributor%20Covenant-v2.0%20adopted-ff69b4.svg
   :target: https://lightbus.org/reference/code-of-conduct/
   :alt: Contributor Covenant


Lightbus allows your backend processes to communicate, run background tasks,
and expose internal APIs.

Lightbus uses Redis as its underlying transport, although support
for other platforms may eventually be added.

Lightbus requires Python 3.7 or above.

**Full documentation can be found at https://lightbus.org**

Designed for ease of use
------------------------

Lightbus is designed with developers in mind. The syntax aims to
be intuitive and familiar, and common problems are caught with
clear and helpful error messages.

For example, a naïve authentication API:

.. code-block:: python3

   class AuthApi(Api):
       user_registered = Event(parameters=('username', 'email'))

       class Meta:
           name = 'auth'

       def check_password(self, user, password):
           return (
               user == 'admin'
               and password == 'secret'
           )

This can be called as follows:

.. code-block:: python3

   import lightbus

   bus = lightbus.create()

   bus.auth.check_password(
       user='admin',
       password='secret'
   )
   # Returns true

You can also listen for events:

.. code-block:: python3

   import lightbus

   bus = lightbus.create()

   def send_signup_email(event_message,
                         username, email):
       send_mail(email,
           subject=f'Welcome {username}'
       )

   @bus.client.on_start()
   def bus_start():
       bus.auth.user_registered.listen(
           send_signup_email
       )

**To get started checkout the documentation at https://lightbus.org.**
