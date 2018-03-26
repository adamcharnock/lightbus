# What is Lightbus?

Lightbus allows your backend processes to communicate, run background tasks,
and expose internal APIs.

Lightbus uses Redis as its underlying transport, although support
for other platforms may eventually be added.

Lightbus requires Python 3.6 or above.

## Designed for ease of use

Lightbus is designed with developers in mind. The syntax aims to
be intuitive and familiar, and common problems are caught with
clear and helpful error messages.

For example, a naive authentication API:

```python
class AuthApi(Api):
    class Meta:
        name = 'auth'

    def check_password(self, user, password):
        return user == 'admin' and password == 'secret'
```

This can be called as follows:

```python
bus = lightbus.create()
bus.auth.check_password(user='admin', password='secret')
# True
bus.auth.check_password(user='admin', password='wrong')
# False
```
