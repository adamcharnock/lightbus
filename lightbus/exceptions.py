class LightbusException(Exception):
    pass


class InvalidMessage(LightbusException):
    pass


class InvalidBusPathConfiguration(LightbusException):
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


class LightbusWorkerError(LightbusException):
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


class FailedToImportBusModule(LightbusException):
    pass


class NoBusFoundInBusModule(LightbusException):
    pass


class TransportIsClosed(LightbusException):
    pass


class DeformError(LightbusException):
    pass


class ValidationError(LightbusException):
    pass


class InvalidSchedule(LightbusException):
    pass


class TransportsNotInstalled(LightbusException):
    pass


class MustRunInBusThread(LightbusException):
    pass


class MustNotRunInBusThread(LightbusException):
    pass


class LightbusExit(LightbusException):
    pass


class BusAlreadyClosed(LightbusException):
    pass


class LightbusServerMustStartInMainThread(LightbusException):
    pass


class RemoteSchemasNotLoaded(LightbusException):
    pass


class TransportPoolIsClosed(LightbusException):
    pass


class CannotShrinkEmptyPool(LightbusException):
    pass


class PoolProxyError(LightbusException):
    pass


class CannotProxyPrivateMethod(PoolProxyError):
    pass


class CannotProxyProperty(PoolProxyError):
    pass


class CannotProxySynchronousMethod(PoolProxyError):
    pass


class AsyncFunctionOrMethodRequired(LightbusException):
    pass


class ListenersAlreadyStarted(LightbusException):
    pass


class DuplicateListenerName(LightbusException):
    pass
