from typing import Optional, List

from lightbus import BusClient
from lightbus.api import registry
from lightbus.exceptions import (
    InvalidBusPathConfiguration,
    InvalidParameters,
    OnlyAvailableOnRootNode,
)
from lightbus.utilities.async_tools import block

__all__ = ["BusPath"]


class BusPath(object):
    """Represents a path on the bus

    This class provides a higher-level wrapper around the `BusClient` class.
    This wrapper allows for a more ideomatic use of the bus. For example:

        bus.auth.get_user(username='admin')

    Compare this to the lower level equivalent using the `BusClient`:

        bus.client.call_rpc_remote(
            api_name='auth',
            name='get_user',
            kwargs={'username': 'admin'},
        )

    """

    def __init__(self, name: str, *, parent: Optional["BusPath"], client: BusClient):
        if not parent and name:
            raise InvalidBusPathConfiguration("Root client node may not have a name")
        self.name = name
        self.parent = parent
        self.client = client

    def __getattr__(self, item) -> "BusPath":
        return self.__class__(name=item, parent=self, client=self.client)

    def __str__(self):
        return self.fully_qualified_name

    def __repr__(self):
        return "<BusPath {}>".format(self.fully_qualified_name)

    def __dir__(self):
        path = [node.name for node in self.ancestors(include_self=True)]
        path.reverse()

        api_names = [[""] + n.split(".") for n in registry.names()]

        matches = []
        apis = []
        for api_name in api_names:
            if api_name == path:
                # Api name matches exactly
                apis.append(api_name)
            elif api_name[: len(path)] == path:
                # Partial API match
                matches.append(api_name[len(path)])

        for api_name in apis:
            api = registry.get(".".join(api_name[1:]))
            matches.extend(dir(api))

        return matches

    # RPC

    def __call__(self, **kwargs):
        return self.call(**kwargs)

    def call(self, *, bus_options: dict = None, **kwargs):
        # Use a larger value of `rpc_timeout` because call_rpc_remote() should
        # handle timeout
        rpc_timeout = self.client.config.api(self.api_name).rpc_timeout * 1.5
        return block(self.call_async(**kwargs, bus_options=bus_options), timeout=rpc_timeout)

    async def call_async(self, *args, bus_options=None, **kwargs):
        if args:
            raise InvalidParameters(
                f"You have attempted to call the RPC {self.fully_qualified_name} using positional "
                f"arguments. Lightbus requires you use keyword arguments. For example, "
                f"instead of func(1), use func(foo=1)."
            )
        return await self.client.call_rpc_remote(
            api_name=self.api_name, name=self.name, kwargs=kwargs, options=bus_options
        )

    # Events

    async def listen_async(self, listener, *, listener_name: str = None, bus_options: dict = None):
        return await self.client.listen_for_event(
            api_name=self.api_name,
            name=self.name,
            listener=listener,
            listener_name=listener_name,
            options=bus_options,
        )

    def listen(self, listener, *, listener_name: str = None, bus_options: dict = None):
        return block(
            self.listen_async(listener, listener_name=listener_name, bus_options=bus_options),
            timeout=self.client.config.api(self.api_name).event_listener_setup_timeout,
        )

    async def fire_async(self, *args, bus_options: dict = None, **kwargs):
        if args:
            raise InvalidParameters(
                f"You have attempted to fire the event {self.fully_qualified_name} using positional "
                f"arguments. Lightbus requires you use keyword arguments. For example, "
                f"instead of func(1), use func(foo=1)."
            )
        return await self.client.fire_event(
            api_name=self.api_name, name=self.name, kwargs=kwargs, options=bus_options
        )

    def fire(self, *, bus_options: dict = None, **kwargs):
        return block(
            self.fire_async(**kwargs, bus_options=bus_options),
            timeout=self.client.config.api(self.api_name).event_fire_timeout,
        )

    # Utilities

    def ancestors(self, include_self=False):
        parent = self
        while parent is not None:
            if parent != self or include_self:
                yield parent
            parent = parent.parent

    def run_forever(self, consume_rpcs=True):
        self.client.run_forever(consume_rpcs=consume_rpcs)

    @property
    def api_name(self):
        path = [node.name for node in self.ancestors(include_self=False)]
        path.reverse()
        return ".".join(path[1:])

    @property
    def fully_qualified_name(self):
        path = [node.name for node in self.ancestors(include_self=True)]
        path.reverse()
        return ".".join(path[1:])

    # Schema

    @property
    def schema(self):
        """Get the bus schema"""
        if self.parent is None:
            return self.client.schema
        else:
            # TODO: Implement getting schema of child nodes if there is demand
            raise AttributeError(
                "Schema only available on root node. Use bus.schema, not bus.my_api.schema"
            )

    @property
    def parameter_schema(self):
        """Get the parameter JSON schema for the given event or RPC"""
        # TODO: Test
        return self.client.schema.get_event_or_rpc_schema(self.api_name, self.name)["parameters"]

    @property
    def response_schema(self):
        """Get the response JSON schema for the given RPC

        Only RPCs have responses. Accessing this property for an event will result in a
        SchemaNotFound error.
        """
        # TODO: Test
        rpc_schema = self.client.schema.get_rpc_schema(self.api_name, self.name)["response"]
        return rpc_schema["response"]

    def validate_parameters(self, parameters: dict):
        # TODO: Test
        self.client.schema.validate_parameters(self.api_name, self.name, parameters)

    def validate_response(self, response):
        # TODO: Test
        self.client.schema.validate_parameters(self.api_name, self.name, response)
