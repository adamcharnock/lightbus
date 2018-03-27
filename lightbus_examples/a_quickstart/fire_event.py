import lightbus


def send_welcome_email(username, email):
    print(f'Subject: Thanks for signing up {username}')
    print(f'To: {email}')


bus = lightbus.create()
bus.auth.user_registered.listen(send_welcome_email)
