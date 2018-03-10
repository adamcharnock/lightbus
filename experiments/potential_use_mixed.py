from typing import Any


@method(api='my_company.auth')
def check_password(username, password):
    return username == 'admin' and password == 'secret'

user_registered = Event(api='my_company.auth', name='user_registered')

# OR

class AuthApi(Api):
    user_registered = Event()

    class Meta:
        name = 'my_company.auth'

    def check_password(self, username, password):
        return username == 'admin' and password == 'secret'


# In either case you can then call
bus = lightbus.create()
bus.my_company.auth.check_password(username='admin', password='secret')
bus.my_company.auth.user_registered.listen(my_callable)
bus.my_company.auth.events_matching('#').listen(my_callable2)


# Just to go off the deep end for a moment


class CheckPassword(lightbus.Rpc):

    class Meta:
        name = 'my_company.auth.check_password'
        # Seriously? This is cluttering up things
        fixtures = {
            'valid': True,
            'invalid': False,
        }
        schema = {
            'type': 'boolean',
        }

    def handle(self):
        return username == 'admin' and password == 'secret'return


# Ok, take 2


class AuthApi(Api):
    user_registered = Event()

    class Meta:
        name = 'my_company.auth'

    def check_password(self, username, password):
        return username == 'admin' and password == 'secret'


AuthApi.check_password.add_fixture(name='valid', value=True)
AuthApi.check_password.add_fixture(name='invalid', value=False)

# Actually, the fixtures should live with the tests. The schema should
# live with the api definition

# Onwards, take 3


class AuthApi(Api):
    user_registered = Event()

    class Meta:
        name = 'my_company.auth'

    def check_password(self, username, password) -> bool:
        return username == 'admin' and password == 'secret'

    def get_user(self, username):
        return ...


class User(object):
    username: str
    email: str


AuthApi.check_password.add_schema(python=bool)
AuthApi.get_user.add_schema(python=User)
# OR
AuthApi.get_user.add_schema(json='json-schema.json')
# The default is implicitly
AuthApi.get_user.add_schema(python=Any)
# However, in the case of check_password() should we auto pickup
# the typing return type?
# Also, parameters are stored in schema (see below)

# Why a schema
#  - Makes developing consumers easier
#  - Can test output against schema
#  - Can test method use against schema
#       >>> AuthApi.check_password.validate(username='adam', bad_key='secret')
#  - Can validate fixtures against schema

# How should schema be stored
#  - Python stubs
#  - Json Schema
#  - Custom format
#  - In whatever format the schema is provided in
#
# The latter option could imply that we would validate
# data using multiple schema systems. That is more appealing
# than converting schemas.
# However, do we need to support more than on schema type?
# Why two?
#  - Simplicity of python schema (but so simple it is arguably of no use)
#  - JSON schema is popular
#
# We could also feasibly turn basic python types into a
# JSON schema.


# On the server:
>>> bus.get_api_schema()
{
    'my_company.auth': [
        {
            'api': 'my_company.auth',
            'name': 'check_password',
            'arguments': {
                'username': {
                    'description': 'Pulled from docs?',
                    'type': 'string',
                    # We could add our own custom key to store the python-specific type if we want
                    # (handy for making stubs)
                },
                'password': {
                    'description': 'Pulled from docs?',
                    'type': 'string'
                }
            },
            'response': {
                'type': 'boolean'
            }
        }
    ]
}
>> bus.dump_schema(path='./schema.json')
>> bus.dump_schema(path='./schema/', multiple=True)  # Multiple files, one for each api


# Then, on the client:
>>> bus = lightbus.from_schema('./schema.json')
>>> bus = lightbus.from_schema('./schema/', multiple=True)  # Will load all JSON files in directory
>>> bus.my_company.auth.check_password('foo', 'bar')

# The client will work without loading a schema, but the schema
# gives the benefits of sanity checking your code

# Note that loading a schema will put lightbus into strict mode,
# whereby deviating from the schema will cause an error

# Also:
>>> bus.generate_stubs('./.stubs/')  # Generate python stubs (code completion!)
>>> bus.my_company.auth.check_password.help()

# Digging into JSON schema some more will be a good idea if we go this route.
# I think it'll get more complex.

