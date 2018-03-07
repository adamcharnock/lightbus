import inspect
from typing import Optional

from lightbus import Api, Event
from lightbus.exceptions import InvalidApiForSchemaCreation
from lightbus.schema.hints_to_schema import make_response_schema, make_rpc_parameter_schema, make_event_parameter_schema
from lightbus.transports.base import SchemaTransport


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

    def make_schema(self, api: Api):
        return api_to_schema(api)


class Parameter(inspect.Parameter):
    """Describes the name and type of an event parameter"""
    empty = inspect.Parameter.empty

    def __init__(self, name, annotation=empty, *, default=empty):
        super(Parameter, self).__init__(name, inspect.Parameter.KEYWORD_ONLY,
                                        default=default,
                                        annotation=annotation
                                        )


class WildcardParameter(inspect.Parameter):
    """Describes a **kwargs style parameter to an event
    """
    # TODO: Consider removing if not found to be useful
    empty = inspect.Parameter.empty

    def __init__(self):
        super(WildcardParameter, self).__init__(
            name='kwargs',
            kind=inspect.Parameter.VAR_KEYWORD,
            default={},
            annotation=dict
        )


def api_to_schema(api: Api) -> dict:
    schema = {
        'rpcs': {},
        'events': {},
    }

    if isinstance(api, type):
        raise InvalidApiForSchemaCreation(
            "An attempt was made to derive an API schema from a type/class, rather than "
            "from an instance of an API. This is probably because you are passing an API "
            "class to api_to_schema(), rather than an instance of the API class."
        )

    for member_name, member in inspect.getmembers(api):
        if member_name.startswith('_'):
            # Don't create schema from private methods
            continue
        if hasattr(Api, member_name):
            # Don't create schema for methods defined on Api class
            continue

        if inspect.ismethod(member):
            schema['rpcs'][member_name] = {
                'parameters': make_rpc_parameter_schema(api.meta.name, member_name, method=member),
                'response': make_response_schema(api.meta.name, member_name, method=member),
            }
        elif isinstance(member, Event):
            schema['events'][member_name] = {
                'parameters': make_event_parameter_schema(api.meta.name, member_name, event=member),
            }

    return schema
