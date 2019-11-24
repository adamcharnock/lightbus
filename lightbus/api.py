from typing import Dict

from lightbus.exceptions import (
    UnknownApi,
    InvalidApiRegistryEntry,
    EventNotFound,
    MisconfiguredApiOptions,
    InvalidApiEventConfiguration,
)


__all__ = ["Api", "Event"]


class ApiRegistry:
    def __init__(self):
        self._apis: Dict[str, Api] = dict()

    def add(self, api: "Api"):
        if isinstance(api, type):
            raise InvalidApiRegistryEntry(
                "An attempt was made to add a type to the API registry. This "
                "is probably because you are trying to add the API class, rather "
                "than an instance of the API class.\n"
                "\n"
                "Use bus.client.register_api(MyApi()), rather than bus.client.register_api(MyApi)"
            )

        self._apis[api.meta.name] = api

    def get(self, name) -> "Api":
        try:
            return self._apis[name]
        except KeyError:
            raise UnknownApi(
                "An API named '{}' was requested from the registry but the "
                "registry does not recognise it. Maybe the incorrect API name "
                "was specified, or maybe the API has not been registered.".format(name)
            )

    def remove(self, name) -> None:
        try:
            del self._apis[name]
        except KeyError:
            raise UnknownApi(
                "An attempt was made to remove an API named '{}' from the registry, but the API "
                "could not be found. Maybe the incorrect API name "
                "was specified, or maybe the API has not been registered.".format(name)
            )

    def public(self):
        return [api for api in self._apis.values() if not api.meta.internal]

    def internal(self):
        return [api for api in self._apis.values() if api.meta.internal]

    def all(self):
        return list(self._apis.values())

    def names(self):
        return list(self._apis.keys())


class ApiOptions:
    name: str
    internal: bool = False
    version: int = 1

    def __init__(self, options):
        for k, v in options.items():
            if not k.startswith("_"):
                setattr(self, k, v)


class ApiMetaclass(type):
    """ API Metaclass

    Validates options in the API's Meta class and populates the
    API class' `meta` attribute.
    """

    def __init__(cls, name, bases=None, dict_=None):
        is_api_base_class = name == "Api" and not bases
        if is_api_base_class:
            super(ApiMetaclass, cls).__init__(name, bases, dict_)
        else:
            options = dict_.get("Meta", None)
            if options is None:
                raise MisconfiguredApiOptions(
                    f"API class {name} does not contain a class named 'Meta'. Each API definition "
                    f"must contain a child class named 'Meta' which can contain configurations options. "
                    f"For example, the 'name' option is required and specifies "
                    f"the name used to access the API on the bus."
                )
            cls.sanity_check_options(name, options)
            cls.meta = ApiOptions(cls.Meta.__dict__.copy())
            super(ApiMetaclass, cls).__init__(name, bases, dict_)

            if cls.meta.name == "default" or cls.meta.name.startswith("default."):
                raise MisconfiguredApiOptions(
                    f"API class {name} is named 'default', or starts with 'default.'. "
                    f"This is a reserved name and is not allowed, please change it to something else."
                )

    def sanity_check_options(cls, name, options):
        if not getattr(options, "name", None):
            raise MisconfiguredApiOptions(
                "API class {} does not specify a name option with its "
                "'Meta' options."
                "".format(name)
            )


class Api(metaclass=ApiMetaclass):
    class Meta:
        name = None

    def get_event(self, name) -> "Event":
        event = getattr(self, name, None)
        if isinstance(event, Event):
            return event
        else:
            raise EventNotFound("Event named {}.{} could not be found".format(self, name))

    def __str__(self):
        return self.meta.name


class Event:
    def __init__(self, parameters=tuple()):
        # Ensure you update the __copy__() method if adding other instance variables below
        if isinstance(parameters, str):
            raise InvalidApiEventConfiguration(
                f"You appear to have passed a string value of {repr(parameters)} "
                f"for your API's event's parameters. This should be a list or a tuple, "
                f"not a string. You probably missed a comma when defining your "
                f"tuple of parameter names."
            )
        self.parameters = parameters
