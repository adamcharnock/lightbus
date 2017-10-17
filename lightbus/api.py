from typing import Dict

from lightbus.exceptions import UnknownApi, InvalidApiRegistryEntry, EventNotFound


class Registry(object):
    def __init__(self):
        self._apis: Dict[str, Api] = dict()

    def add(self, name: str, api: 'Api'):
        if isinstance(api, type):
            raise InvalidApiRegistryEntry(
                "An attempt was made to add a type to the API registry. This "
                "is probably because you are trying to add the API class, rather "
                "than an instance of the API class."
            )

        self._apis[name] = api

    def get(self, name) -> 'Api':
        try:
            return self._apis[name]
        except KeyError:
            raise UnknownApi(
                "An API named '{}' was requested from the registry but the "
                "registry does not recognise it. Maybe the incorrect API name "
                "was specified, or maybe the API has not been registered.".format(name)
            )

    def __iter__(self):
        return iter(self._apis.values())

    def all(self):
        return self._apis.values()


registry = Registry()


class ApiOptions(object):
    name: str

    def __init__(self, options):
        for k, v in options.items():
            if not k.startswith('_'):
                setattr(self, k, v)


class ApiMetaclass(type):

    def __init__(cls, name, bases=None, dict=None):
        is_api_base_class = (name == 'Api' and bases == (object,))
        if is_api_base_class:
            super(ApiMetaclass, cls).__init__(name, bases, dict)
        else:
            options = dict.get('Meta', object())
            cls.meta = ApiOptions(cls.Meta.__dict__.copy())
            super(ApiMetaclass, cls).__init__(name, bases, dict)
            registry.add(options.name, cls())


class Api(object, metaclass=ApiMetaclass):

    class Meta:
        name = None

    async def call(self, procedure_name, kwargs):
        # TODO: Handling code for sync/async method calls (if we want to support both)
        return getattr(self, procedure_name)(**kwargs)

    def get_event(self, name) -> 'Event':
        event = getattr(self, name)
        if isinstance(event, Event):
            return event
        else:
            raise EventNotFound("Event named {}.{} could not be found".format(self, name))

    def __str__(self):
        return self.meta.name


class Event(object):

    def __init__(self, arguments):
        # Ensure you update the __copy__() method if adding instance variables below
        self.arguments = arguments
        # self._api = None

    # def __get__(self, api_instance, type=None):
    #     self._bind_api(api_instance)
    #     return self
    #
    # def __copy__(self):
    #     """Produce an unbound copy of this event"""
    #     return Event(arguments=self.arguments)
    #
    # def dispatch(self, **kwargs):
    #     # Correction, perhaps the event should not do the dispatching.
    #     # Perhaps the client should proxy through calls to events and
    #     # just read metadata from the event.
    #     pass
    #     import pdb; pdb.set_trace()
    #
    # def listen(self):
    #     pass
    #
    # def _bind_api(self, api_instance):
    #     if not isinstance(api_instance, Api):
    #         raise InvalidApiEventConfiguration(
    #             "It appears you are trying to use an event attached to "
    #             "non-API object {}. Events should be defined as class "
    #             "attributes upon your Api class(es). See the API definition "
    #             "documentation for further details.".format(api_instance)
    #         )
    #     if self._api is not None and self._api is not api_instance:
    #         raise InvalidApiEventConfiguration(
    #             "Attempting to use the same Event instance upon two different "
    #             "Apis is unsupported. If you really wish to do this you "
    #             "should copy the event."
    #         )
    #     if self._api is None:
    #         self._api = api_instance
