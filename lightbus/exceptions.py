

class LightbusException(Exception):
    pass


class InvalidRpcMessage(LightbusException):
    pass


class InvalidClientNodeConfiguration(LightbusException):
    pass


class UnsupportedUse(LightbusException):
    pass


class UnknownApi(LightbusException):
    pass


class InvalidApiRegistryEntry(LightbusException):
    pass
