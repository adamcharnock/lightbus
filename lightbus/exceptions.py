

class LightbusException(Exception):
    pass


class InvalidMessage(LightbusException):
    pass


class InvalidBusNodeConfiguration(LightbusException):
    pass


class InvalidSerializerConfiguration(LightbusException):
    pass


class UnsupportedUse(LightbusException):
    pass


class UnknownApi(LightbusException):
    pass


class InvalidApiRegistryEntry(LightbusException):
    pass


class InvalidApiEventConfiguration(LightbusException):
    pass


class MisconfiguredApiOptions(LightbusException):
    pass


class EventNotFound(LightbusException):
    pass


class InvalidEventArguments(LightbusException):
    pass


class InvalidEventListener(LightbusException):
    pass


class NothingToListenFor(LightbusException):
    pass


class CannotBlockHere(LightbusException):
    pass


class LightbusTimeout(LightbusException):
    pass


class SuddenDeathException(LightbusException):
    """Used to kill an invocation for testing purposes"""
    pass


class PluginsNotLoaded(LightbusException):
    pass


class InvalidPlugins(LightbusException):
    pass


class PluginHookNotFound(LightbusException):
    pass


class LightbusServerError(LightbusException):
    pass


class LightbusShutdownInProgress(LightbusException):
    pass
