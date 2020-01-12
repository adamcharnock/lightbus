from itertools import chain
from typing import NamedTuple, Dict, Sequence, List, Set, Type, Union, TYPE_CHECKING, Tuple

from lightbus.exceptions import TransportNotFound, TransportsNotInstalled
from lightbus.transports.pool import TransportPool
from lightbus.utilities.importing import load_entrypoint_classes

empty = NamedTuple("Empty")

if TYPE_CHECKING:
    # pylint: disable=unused-import,cyclic-import
    from lightbus.config import Config
    from lightbus.transports import (
        RpcTransport,
        ResultTransport,
        EventTransport,
        SchemaTransport,
        Transport,
    )

EventTransportPoolType = TransportPool["EventTransport"]
RpcTransportPoolType = TransportPool["RpcTransport"]
ResultTransportPoolType = TransportPool["ResultTransport"]
SchemaTransportPoolType = TransportPool["SchemaTransport"]
AnyTransportPoolType = Union[
    EventTransportPoolType, RpcTransportPoolType, ResultTransportPoolType, SchemaTransportPoolType
]


class TransportRegistry:
    """ Manages access to transports

    It is possible for different APIs within lightbus to use different transports.
    This registry handles the logic of loading the transports for a given
    configuration. Thereafter, it provides access to these transports based on
    a given API.

    The 'default' API is a special case as it is fallback transport for
    any APIs that do not have their own specific transports configured.
    """

    class _RegistryEntry(NamedTuple):
        rpc: RpcTransportPoolType = None
        result: ResultTransportPoolType = None
        event: EventTransportPoolType = None

    schema_transport: TransportPool = None

    def __init__(self):
        self._registry: Dict[str, TransportRegistry._RegistryEntry] = {}

    def load_config(self, config: "Config") -> "TransportRegistry":
        # For every configured API...
        for api_name, api_config in config.apis().items():
            # ...and for each type of transport...
            for transport_type in ("event", "rpc", "result"):
                # ...get the transport config...
                transport_selector = getattr(api_config, f"{transport_type}_transport")
                transport_config = self._get_transport_config(transport_selector)
                # ... and use it to create the transport.
                if transport_config:
                    transport_name, transport_config = transport_config
                    transport_class = get_transport(type_=transport_type, name=transport_name)
                    self._set_transport(
                        api_name, transport_class, transport_type, transport_config, config
                    )

        # Schema transport
        transport_config = self._get_transport_config(config.bus().schema.transport)
        if transport_config:
            transport_name, transport_config = transport_config
            transport_class = get_transport(type_="schema", name=transport_name)
            self.schema_transport = self._instantiate_transport_pool(
                transport_class, transport_config, config
            )

        return self

    def _get_transport_config(self, transport_selector):
        if transport_selector:
            for transport_name in transport_selector._fields:
                transport_config = getattr(transport_selector, transport_name)
                if transport_config is not None:
                    return transport_name, transport_config

    def _instantiate_transport_pool(
        self, transport_class: Type["Transport"], transport_config: NamedTuple, config: "Config"
    ):
        transport_pool = TransportPool(
            transport_class=transport_class, transport_config=transport_config, config=config
        )
        return transport_pool

    def _set_transport(
        self,
        api_name: str,
        transport_class: Type["Transport"],
        transport_type: str,
        transport_config: NamedTuple,
        config: "Config",
    ):
        """Set the transport pool for a specific API"""
        from lightbus.transports import Transport

        assert issubclass(
            transport_class, Transport
        ), f"Must be a subclass for Transport, was {transport_class}"

        self._registry.setdefault(api_name, self._RegistryEntry())
        transport_pool = self._instantiate_transport_pool(transport_class, transport_config, config)
        self._registry[api_name] = self._registry[api_name]._replace(
            **{transport_type: transport_pool}
        )

    def _get_transport_pool(
        self, api_name: str, transport_type: str, default=empty
    ) -> AnyTransportPoolType:
        # Get the registry entry for this API (if any)
        registry_entry = self._registry.get(api_name)
        api_transport = None

        # If we have a registry entry for this API, then get the transport for it
        if registry_entry:
            api_transport = getattr(registry_entry, transport_type)

        # Otherwise get the transport for the default API (which is always our fallback)
        # (but don't bother if they have explicity asked for the default_api, as if they
        # have then we've already failed to get that in the previous step)
        if not api_transport and api_name != "default":
            try:
                api_transport = self._get_transport_pool("default", transport_type)
            except TransportNotFound:
                pass

        # If we STILL don't have a transport then show a sensible error
        if not api_transport and default == empty:
            raise TransportNotFound(
                f"No {transport_type} transport found for API '{api_name}'. Neither was a default "
                f"API transport found. Either specify a {transport_type} transport for this specific API, "
                f"or specify a default {transport_type} transport. In most cases setting a default transport "
                f"is the best course of action."
            )
        else:
            return api_transport

    def _get_transport_pools(
        self, api_names: Sequence[str], transport_type: str
    ) -> Dict[AnyTransportPoolType, List[str]]:
        apis_by_transport: Dict[AnyTransportPoolType, List[str]] = {}
        for api_name in api_names:
            transport = self._get_transport_pool(api_name, transport_type)
            apis_by_transport.setdefault(transport, [])
            apis_by_transport[transport].append(api_name)
        return apis_by_transport

    def _has_transport(self, api_name: str, transport_type: str) -> bool:
        try:
            self._get_transport_pool(api_name, transport_type)
        except TransportNotFound:
            return False
        else:
            return True

    def set_rpc_transport(
        self,
        api_name: str,
        transport_class: Type["RpcTransport"],
        transport_config: NamedTuple,
        config: "Config",
    ):
        self._set_transport(api_name, transport_class, "rpc", transport_config, config)

    def set_result_transport(
        self,
        api_name: str,
        transport_class: Type["ResultTransport"],
        transport_config: NamedTuple,
        config: "Config",
    ):
        self._set_transport(api_name, transport_class, "result", transport_config, config)

    def set_event_transport(
        self,
        api_name: str,
        transport_class: Type["EventTransport"],
        transport_config: NamedTuple,
        config: "Config",
    ):
        self._set_transport(api_name, transport_class, "event", transport_config, config)

    def set_schema_transport(
        self,
        transport_class: Type["SchemaTransport"],
        transport_config: NamedTuple,
        config: "Config",
    ):
        self.schema_transport = self._instantiate_transport_pool(
            transport_class, transport_config, config
        )

    def get_rpc_transport(self, api_name: str, default=empty) -> RpcTransportPoolType:
        return self._get_transport_pool(api_name, "rpc", default=default)

    def get_result_transport(self, api_name: str, default=empty) -> ResultTransportPoolType:
        return self._get_transport_pool(api_name, "result", default=default)

    def get_event_transport(self, api_name: str, default=empty) -> EventTransportPoolType:
        return self._get_transport_pool(api_name, "event", default=default)

    def get_all_rpc_transports(self) -> Set[RpcTransportPoolType]:
        return {t.rpc for t in self._registry.values() if t.rpc}

    def get_all_result_transports(self) -> Set[ResultTransportPoolType]:
        return {t.result for t in self._registry.values() if t.result}

    def get_all_event_transports(self) -> Set[EventTransportPoolType]:
        return {t.event for t in self._registry.values() if t.event}

    def get_schema_transport(self, default=empty) -> SchemaTransportPoolType:
        if self.schema_transport or default != empty:
            return self.schema_transport or default
        else:
            # TODO: Link to docs
            raise TransportNotFound(
                "No schema transport is configured for this bus. Check your schema transport "
                "configuration is setup correctly (config section: bus.schema.transport)."
            )

    def has_rpc_transport(self, api_name: str) -> bool:
        return self._has_transport(api_name, "rpc")

    def has_result_transport(self, api_name: str) -> bool:
        return self._has_transport(api_name, "result")

    def has_event_transport(self, api_name: str) -> bool:
        return self._has_transport(api_name, "event")

    def has_schema_transport(self) -> bool:
        return bool(self.schema_transport)

    def get_rpc_transports(self, api_names: Sequence[str]) -> Dict[RpcTransportPoolType, List[str]]:
        """Get a mapping of transports to lists of APIs

        This is useful when multiple APIs can be served by a single transport
        """
        return self._get_transport_pools(api_names, "rpc")

    def get_event_transports(
        self, api_names: Sequence[str]
    ) -> Dict[EventTransportPoolType, List[str]]:
        """Get a mapping of transports to lists of APIs

        This is useful when multiple APIs can be served by a single transport
        """
        return self._get_transport_pools(api_names, "event")

    def get_all_transports(self) -> Set[AnyTransportPoolType]:
        """Get a set of all transports irrespective of type"""
        all_transports = chain(*[entry._asdict().values() for entry in self._registry.values()])
        return set([t for t in all_transports if t is not None])


def get_available_transports(type_):
    loaded = load_entrypoint_classes(f"lightbus_{type_}_transports")

    if not loaded:
        raise TransportsNotInstalled(
            f"No {type_} transports are available, which means lightbus has not been "
            f"installed correctly. This is likely because you are working on Lightbus itself. "
            f"In which case, within your local lightbus repo you should run "
            f"something like 'pip install .' or 'python setup.py develop'.\n\n"
            f"This will install the entrypoints (defined in setup.py) which point Lightbus "
            f"to it's bundled transports."
        )
    return {name: class_ for module_name, name, class_ in loaded}


def get_transport(type_, name):
    for name_, class_ in get_available_transports(type_).items():
        if name == name_:
            return class_

    raise TransportNotFound(
        f"No '{type_}' transport found named '{name}'. Check the transport is installed and "
        f"has the relevant entrypoints setup in it's setup.py file. Or perhaps "
        f"you have a typo in your config file."
    )


def get_transport_name(cls: Type[AnyTransportPoolType]):
    for type_ in ("rpc", "result", "event"):
        for *_, name, class_ in load_entrypoint_classes(f"lightbus_{type_}_transports"):
            if cls == class_:
                return name

    raise TransportNotFound(
        f"Transport class {cls.__module__}.{cls.__name__} is not specified in any entrypoint."
    )
