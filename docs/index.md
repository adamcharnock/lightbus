<style>

</style>

# What is Lightbus?

Lightbus is a powerful and intuitive messaging client for your
backend Python services.

Lightbus uses Redis 5 as its underlying [transport](explanation/transports.md), although support
for other platforms will be added in future.

Other languages can also communicate with Lightbus by 
[interacting with Redis](reference/protocols/index.md).

## How Lightbus works

Lightbus provides you with two tools:

1. A **client** with which to fire events,
  and make remote procedure calls (RPCs) from anywhere within your
  codebase.
1. A **stand-alone Lightbus worker process** in which you can setup
  event listeners. This process will also respond to RPCs calls.

For example, you could architect an e-commerce system as follows:

![A simple Lightbus deployment][simple-processes]

In this example:

* **Django** serves pages using data from the database
* **Django** performs remote procedure calls to resize images. The Lightbus
  worker in the **image resizing service** performs the image resize and responds.
* The **price monitoring service** fires `price_monitor.competitor_price_changed` events
* The Lightbus worker in the **online shop web service** listens for
  `price_monitor.competitor_price_changed` events and updates prices in the
  database accordingly.


See the [anatomy lesson] for further discussion.

## Designed for ease of use

Lightbus is designed to be intuitive and familiar,
and common problems are caught with
clear and helpful error messages.

For example, a naïve authentication API:

```python3
class AuthApi(Api):
    user_registered = Event(parameters=('user', 'email'))

    class Meta:
        name = 'auth'

    def check_password(self, user, password):
        return (
            user == 'admin'
            and password == 'secret'
        )
```

The `check_password` procedure can be called remotely as follows:

```python3
import lightbus

bus = lightbus.create()

is_valid = bus.auth.check_password(
    user='admin',
    password='secret'
)
# is_valid is True
```

You can also listen for events:

```python3
# bus.py
import lightbus

bus = lightbus.create()

# Our event handler
def send_signup_email(event_message,
                      user, email):
    send_mail(email,
        subject=f'Welcome {user}'
    )

# Setup our listeners on startup
@bus.client.on_start()
def on_start():
    bus.auth.user_registered.listen(
        send_signup_email,
        listener_name="send_signup_email"
    )
```

## Where to start?

Starting with the **[tutorials] section** will give you a
**practical introduction** to Lightbus.
Alternatively, the **[explanation] section** will give you a
grounding in the high level **concepts and theory**.

Start with whichever section suits you best. You should
ultimately look through both sections for a complete understanding.

In addition, **the [how to] section gives solutions to common 
use cases**, and **the [reference] section provides detailed 
technical information** regarding specific features.

## Questions?

Get in touch via:

* Email: adam@adamcharnock.com
* Phone: +442032896620 (Skype, London/Lisbon timezone)
* [Community chat](https://discord.gg/2j594ws)
* GitHub: https://github.com/adamcharnock/lightbus/  

If you are having a technical problem then the more information 
you can include the better (problem description, screenshots, and code 
are all useful).

[issue-1]: https://github.com/adamcharnock/lightbus/issues/1
[simple-processes]: /static/images/simple-processes.png
[anatomy lesson]: explanation/anatomy-lesson.md
[tutorials]: tutorial/index.md
[explanation]: explanation/index.md
[How to]: howto/index.md
[Reference]: reference/index.md
