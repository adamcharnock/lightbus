import logging
from contextlib import contextmanager

import lightbus
import lightbus.transports.debug
from lightbus.api import Api
from lightbus.exceptions import InvalidApiEventConfiguration
from lightbus.log import LBullets, L, Bold
from lightbus.utilities import setup_dev_logging


class Event(object):
    
    def __init__(self, arguments):
        # Ensure you update the __copy__() method if adding instance variables below
        self.arguments = arguments
        self._api = None
        self._bus = None

    def __get__(self, api_instance, type=None):
        self._bind_api(api_instance)
        return self

    def __copy__(self):
        """Produce an unbound copy of this event"""
        return Event(arguments=self.arguments)

    def dispatch(self, **kwargs):
        # Correction, perhaps the event should not do the dispatching.
        # Perhaps the client should proxy through calls to events and
        # just read metadata from the event.
        pass
        import pdb; pdb.set_trace()

    def listen(self):
        pass

    def _bind_api(self, api_instance):
        if not isinstance(api_instance, Api):
            raise InvalidApiEventConfiguration(
                "It appears you are trying to use an event attached to "
                "non-API object {}. Events should be defined as class "
                "attributes upon your Api class(es). See the API definition "
                "documentation for further details.".format(api_instance)
            )
        if self._api is not None and self._api is not api_instance:
            raise InvalidApiEventConfiguration(
                "Attempting to use the same Event instance upon two different "
                "Apis is unsupported. If you really wish to do this you "
                "should copy the event."
            )
        if self._api is None:
            self._api = api_instance

    @contextmanager
    def _bind_bus(self, bus):
        self._bus = bus
        yield
        self._bus = None


class AuthApi(Api):
    user_registered = Event(arguments=['username'])

    class Meta:
        name = 'my_company.auth'

    def check_password(self, username, password):
        return username == 'admin' and password == 'secret'


def main():
    setup_dev_logging()

    bus = lightbus.Bus(
        rpc_transport=lightbus.transports.RedisRpcTransport(),
        result_transport=lightbus.transports.RedisResultTransport()
    )
    api = AuthApi()
    api.user_registered
    api.user_registered
    bus.serve(api)


if __name__ == '__main__':
    main()
