Lightbus is designed to work smoothly with Django. A few of additions are needed 
to your `bus.py` file in order to setup django correctly. You should 
perform these at the very start of your `@bus.client.on_start()` function 
within your `bus.py`:

1. Set your `DJANGO_SETTINGS_MODULE` environment variable[^1]
1. Call `django.setup()`
1. Decorate RPCs and handlers which use the Django ORM with `@uses_django_db`. This 
   will ensure database errors are cleaned up correctly.

## Simple example

The following example demonstrates:

1. Setting `DJANGO_SETTINGS_MODULE`
2. Calling `django.setup()`

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

Here we build upon the simple example (above). 
This example includes an API and an event handler, and is loosely based upon our work 
in the [quick start tutorial](quick-start.md). Here we demonstrate:

1. Setting `DJANGO_SETTINGS_MODULE`
2. Calling `django.setup()`
3. Applying `@uses_django_db` to an RPC and event handler

```python
import lightbus
from lightbus.utilities.django import uses_django_db
import django
import os

bus = lightbus.create()

class AuthApi(lightbus.Api):
    user_registered = lightbus.Event(parameters=('username', 'email'))

    class Meta:
        name = 'auth'
    
    # Decorating our RPC with @uses_django_db  
    # ensures database connections are cleaned up
    @uses_django_db
    def check_password(self, username, password):
        # Import django models here, once django.setup() has been called
        from django.contrib.auth.models import User
        try:
            user = User.objects.get(username=username)
            return user.check_password(password)
        except User.DoesNotExist:
            return False

bus.client.register_api(AuthApi())


# Decorating our event handler with @uses_django_db  
# ensures database connections are cleaned up
@uses_django_db
def handle_new_user(event, username, email):
    # For example, we may want to create an audit log entry
    from my_app.models import AuditLogEntry
    AuditLogEntry.objects.create(message=f"User {username} created")


@bus.client.on_start()
def on_start(**kwargs):
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "my_company.a_project.settings")
    django.setup()
    
    # Setup a listener for the 'auth.user_registered' event, to be 
    # handled by handle_new_user() (above)
    bus.auth.user_registered.listen(
        handle_new_user,
        listener_name="create_audit_entry"
    )
```

---

!!! note "Further development"

    Simplification to the Django setup process is 
    [tracked in GitHub](https://github.com/adamcharnock/lightbus/issues/6).

[^1]: You will see that your Django project' `manage.py` file also performs this same step
