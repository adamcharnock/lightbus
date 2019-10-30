# How to migrate from Celery to Lightbus

Migration from Celery to Lightbus can be a very straightforward process depending on how 
you currently make use of celery. However, before you start out you should take time to 
familiarise yourself with the principles behind Lightbus, and its differences to Celery:

* Lightbus [anatomy lesson](../explanation/anatomy-lesson.md)
* [Lightbus vs Celery](../explanation/lightbus-vs-celery.md)

## Queued tasks

Celery is often used to schedule background tasks for later processing. For example, a 
web server may schedule a task to send a welcome email to a new user. This task will go into 
your Celery queue and be handled by a Celery worker process. The web process does not need 
any response from the worker, it only needs to know that it *will* be handled.

**In Lightbus these are [events].**

We can convert this to Lightbus as follows:

```python3
# bus.py
import lightbus

# Create an API for everything user-related
class UserApi(lightbus.Api):
    # We specify the event which has occurred (a user signed up)
    user_signed_up = lightbus.Event(parameters=('email', 'username'))

    class Meta:
        # Name our API 
        # (used below to setup our listener)
        name = "user"


# Create our bus client (this is a requirement of our bus.py file)
bus = lightbus.create()

# Register the API (see docs: Explanation -> APIs)
bus.client.register_api(UserApi())

@bus.client.on_start()
def bus_start(**kwargs):
    from my_app.somewhere import send_welcome_email
    # Listen for this event being fired.
    # When the event is fired, send_welcome_email will be called as follows:
    #     send_welcome_email(event, email, username)
    bus.user.user_signed_up.listen(send_welcome_email, listener_name="send_welcome_email")
```

Then, within our web server code we can send an email as follows:

```python3
# Your web server code

# Import the bus client from your bus.py file
from bus import bus

# Your registration view
@route('/register', methods=('GET', 'POST'))
def register():
    if request.method == 'POST':
        # ...validate and create user...
        
        # Send registration email via the Lightbus process
        bus.user.user_signed_up.fire(
            username=request.form['username'], 
            email=request.form['email']
        )
    else:
        # ...
```

This will fire an event on the bus, and our `lightbus run` process will receive this event and send the 
welcome email.
 
## Fetching Celery task results 

Sometimes you wish to have a Celery worker process execute a task for you, and then return the 
result. For example, perhaps you need to get the URL to a resized user avatar image. Image resizing 
is computationally expensive, so you prefer to do this on your Celery workers. You want the worker 
to do this task for you, and the web server will wait for it to be done, and the web server needs a 
response (the image URL) from the worker.

**In Lightbus these are [remote procedure calls] (RPCs).**

We can implement the above example as follows:

```python3
# bus.py (as above, with one addition)
import lightbus

class UserApi(lightbus.Api):
    user_signed_up = lightbus.Event(parameters=("email", "username"))

    class Meta:
        name = "user"
    
    # This is our new remote procedure
    def get_avatar_url(self, username, width, height):
        # ... do some resizing & uploading ...
        return f"https://example.com/images/{width}/{height}/{username}.png"

bus = lightbus.create()
bus.client.register_api(UserApi())

@bus.client.on_start()
def bus_start(**kwargs):
    from my_app.somewhere import send_welcome_email
    bus.user.user_signed_up.listen(send_welcome_email, listener_name="send_welcome_email")
```

We can then use this within our web server as follows:

```python3
# Your web server code

# Import the bus client from your bus.py file
from bus import bus

# Your registration view
@route('/user-profile/<username>', methods=('GET',))
def user_profile(username):
    # Call our new remote procedure
    image_url = bus.user.get_avatar_url(username=username, width=500, height=500)
    return f"<h1>{username}</h1><img src='{image_url}'>"
```

Now, as long as our `lightbus run` process is running, the web server will be able to 
call the remote procedure and render the user profile HTML.

## Scheduled tasks

Lightbus can replace Celery's periodic tasks through the use of the `@bus.client.every()` and/or 
`@bus.client.schedule()` decorators (see [how to schedule recurring tasks]). 

For example:

```python3
# bus.py
import lightbus

bus = lightbus.create()

@bus.client.every(hours=1)
def every_hour():
    # Will be called every hour
    refresh_customer_data()
```

The `lightbus run` command will then execute this function every hour.

**See [how to schedule recurring tasks]** for more details on the available scheduling options.



[how to schedule recurring tasks]: schedule-recurring-tasks.md
[events]: ../explanation/events.md
[remote procedure calls]: ../explanation/rpcs.md
