from typing import Optional

from lightbus import Api
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

