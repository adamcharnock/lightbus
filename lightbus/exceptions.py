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


class InvalidApiForSchemaCreation(LightbusException):
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


class InvalidSchema(LightbusException):
    pass


class SchemaNotFound(LightbusException):
    pass


class NoResponseSchemaFound(SchemaNotFound):
    pass


class TransportNotFound(LightbusException):
    pass


class TooManyTransportsForApi(LightbusException):
    pass


class NoTransportConfigured(LightbusException):
    pass


class NoApisToListenOn(LightbusException):
    pass


class InvalidName(LightbusException):
    pass


class InvalidParameters(LightbusException):
    pass


class ApisMustUseSameTransport(LightbusException):
    pass


class OnlyAvailableOnRootNode(LightbusException):
    pass


class UnexpectedConfigurationFormat(LightbusException):
    pass


class UnsupportedOptionValue(LightbusException):
    pass


class DatabaseConnectionRequired(LightbusException):
    pass


class DuplicateMessage(LightbusException):
    pass


class NoApisSpecified(LightbusException):
    pass
