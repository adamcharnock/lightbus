import inspect
from typing import Optional, NamedTuple

from lightbus import Api, Event, Api
from lightbus.transports.base import SchemaTransport

# schema = {
#     'my.api': {
#         'rpcs': {
#             'check_password': {
#                 'parameters': SCHEMA,
#                 'response': SCHEMA,
#             }
#         },
#         'events': {
#             'user_registered': {
#                 'parameters': SCHEMA,
#             }
#         },
#     }
# }


class RegistrationEventParameters(NamedTuple):
    username: str
    email: str
    is_admin: str = False


class TestApi(Api):
    # The current implementation
    user_registered1 = Event(parameters=['username', 'email', 'is_admin'])

    # Simple, no default values, no **kwargs
    user_registered2 = Event(parameters={'username': str, 'email': str, 'is_admin': bool})

    # Verbose, but has default values and **kwargs
    user_registered3 = Event(parameters=[
        inspect.Parameter('username', kind=inspect.Parameter.KEYWORD_ONLY, annotation=str),
        inspect.Parameter('email', kind=inspect.Parameter.KEYWORD_ONLY, annotation=str),
        inspect.Parameter('is_admin', kind=inspect.Parameter.KEYWORD_ONLY, annotation=str),
        inspect.Parameter('extra_fields', kind=inspect.Parameter.VAR_KEYWORD, annotation=str, default=False),
    ])

    # As above, but customised for more concise definitions
    user_registered4 = Event(parameters=[
        Parameter('username', str),
        Parameter('email', str),
        Parameter('is_admin', str),
        WildcardParameter('extra_fields', str, default=False),
    ])

    # Does not support **kwargs. Parameters can no longer be inline
    user_registered5 = Event(parameters=RegistrationEventParameters)


class Schema(object):
    """ Represents the bus' schema


    Note that the presence of a schema does not necessarily
    indicate that a lightbus process is present or ready to serve
    requests for the API. For that you will need to consume the events
    produced by the state plugin.

    That being said, you should expect old schemas to be dropped
    after max_age_seconds.
    """
    # TODO: Periodically renew the schema within the transport (as per max_age_seconds)
    # TODO: Reload schemas when a new lightbus process comes online

    def __init__(self, schema_transport: SchemaTransport, max_age_seconds: Optional[int]=3600 * 24):
        self.schema_transport = schema_transport
        self.max_age_seconds = max_age_seconds
        self.local_schemas = {}
        self.remote_schemas = {}

    def add_api(self, api: Api):
        # Adds an API locally, and sends to to the transport
        schema = self.make_schema(api)  # TODO: IMPLEMENT IT
        self.local_schemas[api.meta.name] = schema
        self.schema_transport.store(api.meta.name, schema, ttl_seconds=self.max_age_seconds)

    def get_schema(self, api_name) -> Optional[dict]:
        return self.local_schemas.get(api_name) or self.remote_schemas.get(api_name)

