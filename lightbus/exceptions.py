

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


class EventNotFound(LightbusException):
    pass


class InvalidEventArguments(LightbusException):
    pass

