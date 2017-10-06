

class LightbusException(Exception):
    pass


class InvalidRpcMessage(LightbusException):
    pass


class InvalidClientNodeConfiguration(LightbusException):
    pass
