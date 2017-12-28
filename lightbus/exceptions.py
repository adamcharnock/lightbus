

class LightbusException(Exception):
    pass


class InvalidRpcMessage(LightbusException):
    pass


class InvalidBusNodeConfiguration(LightbusException):
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


class CannotBlockHere(LightbusException):
    pass


class InvalidRedisPool(LightbusException):
    pass


class LightbusTimeout(LightbusException):
    pass


class SuddenDeathException(LightbusException):
    """Used to kill an invocation for testing purposes"""
    pass


class PluginsNotLoaded(LightbusException):
    pass


class PluginHookNotFound(LightbusException):
    pass
