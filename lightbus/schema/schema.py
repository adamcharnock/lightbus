import inspect
from typing import Optional

import asyncio

import lightbus
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

    def __init__(self, schema_transport: SchemaTransport, max_age_seconds: Optional[int]=60):
        # TODO: Pull max_age_seconds from configuration
        self.schema_transport = schema_transport
        self.max_age_seconds = max_age_seconds
        self.local_schemas = {}
        self.remote_schemas = {}

    async def add_api(self, api: 'Api'):
        """Adds an API locally, and sends to to the transport"""
        schema = api_to_schema(api)
        self.local_schemas[api.meta.name] = schema
        await self.schema_transport.store(api.meta.name, schema, ttl_seconds=self.max_age_seconds)

    def get_schema(self, api_name) -> Optional[dict]:
        """Get the schema for the given API"""
        return self.local_schemas.get(api_name) or self.remote_schemas.get(api_name)

    async def store(self):
        """Store the schema onto the bus"""
        for api_name, schema in self.local_schemas.items():
            await self.schema_transport.store(api_name, schema, ttl_seconds=self.max_age_seconds)

    async def load(self):
        self.remote_schemas = await self.schema_transport.load()

    async def monitor(self, interval=None):
        """Monitor for remote schema changes and keep any local schemas alive on the bus
        """
        interval = interval or self.max_age_seconds * 0.8
        try:
            while True:
                await asyncio.sleep(interval)
                # Keep alive our local schemas
                for api_name, schema in self.local_schemas.items():
                    await self.schema_transport.ping(api_name, schema, ttl_seconds=self.max_age_seconds)

                # Read the entire schema back from the bus
                await self.load()
        except asyncio.CancelledError:
            return


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


def api_to_schema(api: 'lightbus.Api') -> dict:
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
        if hasattr(lightbus.Api, member_name):
            # Don't create schema for methods defined on Api class
            continue

        if inspect.ismethod(member):
            schema['rpcs'][member_name] = {
                'parameters': make_rpc_parameter_schema(api.meta.name, member_name, method=member),
                'response': make_response_schema(api.meta.name, member_name, method=member),
            }
        elif isinstance(member, lightbus.Event):
            schema['events'][member_name] = {
                'parameters': make_event_parameter_schema(api.meta.name, member_name, event=member),
            }

    return schema
