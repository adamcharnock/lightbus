from typing import Dict

from lightbus.exceptions import UnknownApi, InvalidApiRegistryEntry


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

    def get(self, name):
        try:
            return self._apis[name]
        except KeyError:
            raise UnknownApi(
                "An API named '{}' was requested from the registry but the "
                "registry does not recognise it. Maybe the incorrect API name "
                "was specified, or maybe the API has not been registered.".format(name)
            )


registry = Registry()


class ApiMeta(type):

    def __init__(cls, name, bases=None, dict=None):
        is_api_base_class = (name == 'Api' and bases == (object,))
        if is_api_base_class:
            super(ApiMeta, cls).__init__(name, bases, dict)
        else:
            dict['meta'] = cls.Meta()
            super(ApiMeta, cls).__init__(name, bases, dict)
            registry.add(dict['meta'].name, cls())


class Api(object, metaclass=ApiMeta):

    class Meta:
        name = None

    async def call(self, procedure_name, kwargs):
        # TODO: Handling code for sync/async method calls (if we want to support both)
        return getattr(self, procedure_name)(**kwargs)
