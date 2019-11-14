Lightbus is designed to work smoothly with Django. A couple of additions are needed 
to your `bus.py` file in order to setup django correctly. You should 
perform these at the very start of your `@bus.client.on_start()` function 
within your `bus.py`:

1. Set your `DJANGO_SETTINGS_MODULE` environment variable[^1]
1. Call `django.setup()`

## Simple example

```python
# bus.py
import lightbus
import django
import os
# Do NOT import your django models here

bus = lightbus.create()

# ...Define and register APIs here...

@bus.client.on_start()
def on_start(**kwargs):
    # DJANGO STEP 1: Set DJANGO_SETTINGS_MODULE\
    # Customise this value (if necessary) to point to your settings module. 
    # This should be the same as the corresponding line in your manage.py file.
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
    
    # DJANGO STEP 2: Call django.setup()
    # We must do this before we import any of our django models
    django.setup()

    # You can now safely import your django models here if needed,
    # or within any event handlers we setup (see next example)
    from my_app.models import MyModel
```

## Practical example

Here is a more involved example (with an API and an event handler) based upon our work 
in the [quick start tutorial](quick-start.md):

```python
import lightbus
import django
import os

# Create your service's bus client. You can import this elsewere
# in your service's codebase in order to access the bus
bus = lightbus.create()

# Define our auth Api
class AuthApi(lightbus.Api):
    user_registered = lightbus.Event(parameters=('username', 'email'))

    class Meta:
        name = 'auth'

    def check_password(self, username, password):
        # Import django models here, once django.setup() has been called
        from django.contrib.auth.models import User
        try:
            user = User.objects.get(username=username)
            return user.check_password(password)
        except User.DoesNotExist:
            return False

bus.client.register_api(AuthApi())


def handle_new_user(event, username, email):
    # For example, we may want to create an audit log entry
    from my_app.models import AuditLog
    AuditLog.objects.create(action=f"User {username} created")


@bus.client.on_start()
def on_start(**kwargs):
    # Customise (if necessary) to point to your settings module. 
    # Should be the same as the corresponding line in your manage.py file.
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "my_company.a_project.settings")

    # Trigger django to set itself up. We must do this before we import 
    # any of our django models
    django.setup()
    
    # Setup a listener for the 'auth.user_registered' event, to be 
    # handled by handle_new_user() (above)
    bus.auth.user_registered.listen(
        handle_new_user,
        listener_name="create_audit_entry"
    )

```

[^1]: You will see that your Django project' `manage.py` file also performs this same step
