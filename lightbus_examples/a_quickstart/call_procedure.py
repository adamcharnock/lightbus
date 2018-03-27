import lightbus

bus = lightbus.create()
valid = bus.auth.check_password(
    username=input('Username: '),
    password=input('Password: ')
)
if valid:
    print('Password valid!')
else:
    print('Oops, bad username or password')
