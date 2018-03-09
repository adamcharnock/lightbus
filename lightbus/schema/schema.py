import inspect
import json
from json import JSONDecodeError
from pathlib import Path
from typing import Optional, TextIO, Union, ChainMap

import asyncio

import itertools

import sys

import lightbus
from lightbus.exceptions import InvalidApiForSchemaCreation, InvalidSchema
from lightbus.schema.hints_to_schema import make_response_schema, make_rpc_parameter_schema, make_event_parameter_schema
from lightbus.transports.base import SchemaTransport
from lightbus.utilities import make_file_safe_api_name


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

    @property
    def api_names(self):
        return list(set(itertools.chain(self.local_schemas.keys(), self.remote_schemas.keys())))

    async def save_to_bus(self):
        """Save the schema onto the bus

        This will be done using the `schema_transport` provided to `__init__()`
        """
        for api_name, schema in self.local_schemas.items():
            await self.schema_transport.store(api_name, schema, ttl_seconds=self.max_age_seconds)

    async def load_from_bus(self):
        """Save the schema from the bus

        This will be done using the `schema_transport` provided to `__init__()`
        """
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
                await self.load_from_bus()
        except asyncio.CancelledError:
            return

    def save_local(self, destination: Union[str, Path, TextIO]=None):
        if isinstance(destination, str):
            destination = Path(destination)

        if destination is None:
            self._dump_to_file(sys.stdout)
        elif destination.is_dir():
            self._dump_to_directory(destination)
        else:
            with destination.open('w', encoding='utf8') as f:
                self._dump_to_file(f)

    def load_local(self, source: Union[str, Path, TextIO]=None):
        if isinstance(source, str):
            source = Path(source)

        if source is None:
            json_schema = sys.stdin.read()
            try:
                schema = json.loads(json_schema)
            except JSONDecodeError as e:
                raise InvalidSchema('Could not parse schema received on stdin: {}'.format(e.msg))
        elif source.is_dir():
            schemas = []
            for file_path in source.glob('*.json'):
                try:
                    schema = json.loads(file_path.read())
                except JSONDecodeError as e:
                    raise InvalidSchema('Could not parse schema file at {}: {}'.format(
                        file_path, e.msg
                    ))
                schemas.append(schema)
            schema = ChainMap(*schemas)
        else:
            with source.open('r', encoding='utf8') as f:
                try:
                    schema = json.loads(f.read())
                except JSONDecodeError as e:
                    raise InvalidSchema('Could not parse schema file at {}: {}'.format(
                        source, e.msg
                    ))

        for api_name, api_schema in schema.items():
            self.schema_transport.store(api_name, api_schema, ttl_seconds=self.max_age_seconds)

        return schema

    def _dump_to_directory(self, destination: Path):
        for api_name in self.api_names:
            file_name = '{}.json'.format(make_file_safe_api_name(api_name))
            (destination / file_name).write_text(self._get_dump(api_name), encoding='utf8')

    def _dump_to_file(self, f):
        f.write(self._get_dump())

    def _get_dump(self, api_name=None):
        if api_name:
            schema = {api_name: self.get_schema(api_name)}
        else:
            schema = {api_name: self.get_schema(api_name) for api_name in self.api_names}
        return json.dumps(schema, indent=2, sort_keys=True) + "\n"


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
