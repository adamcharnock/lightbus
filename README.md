# What is Lightbus?

[![CircleCI](https://circleci.com/gh/adamcharnock/lightbus/tree/master.svg?style=svg)](https://circleci.com/gh/adamcharnock/lightbus/tree/master)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/801d031fd2714b4f9c643182f1fbbd0b)](https://www.codacy.com/app/adamcharnock/lightbus?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=adamcharnock/lightbus&amp;utm_campaign=Badge_Grade)
[![Codacy Badge](https://api.codacy.com/project/badge/Coverage/801d031fd2714b4f9c643182f1fbbd0b)](https://www.codacy.com/app/adamcharnock/lightbus?utm_source=github.com&utm_medium=referral&utm_content=adamcharnock/lightbus&utm_campaign=Badge_Coverage)

Lightbus allows your backend processes to communicate, run background tasks,
and expose internal APIs.

Lightbus uses Redis as its underlying transport, although support
for other platforms may eventually be added.

Lightbus requires Python 3.6 or above.

**Lightbus is under active development and is still pre-release.**
You can [track progress in GitHub][issue-1].

## Designed for ease of use

Lightbus is designed with developers in mind. The syntax aims to
be intuitive and familiar, and common problems are caught with
clear and helpful error messages.

For example, a naïve authentication API:

```python3
class AuthApi(Api):
    user_registered = Event(parameters=('username', 'email'))

    class Meta:
        name = 'auth'

    def check_password(self, user, password):
        return (
            user == 'admin'
            and password == 'secret'
        )
```

This can be called as follows:

```python3
import lightbus

bus = lightbus.create()

bus.auth.check_password(
    user='admin',
    password='secret'
)
# Returns true
```

You can also listen for events:

```python3
def send_signup_email(event_message,
                      username, email):
    send_mail(email,
        subject=f'Welcome {username}'
    )

bus.auth.user_registered.listen(
    send_signup_email
)
```

To get started checkout the documentation at https://lightbus.org.

[issue-1]: https://github.com/adamcharnock/lightbus/issues/1
