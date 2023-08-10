import traceback
from typing import Optional, Dict, Any, Sequence
from uuid import uuid1


__all__ = ["Message", "RpcMessage", "ResultMessage", "EventMessage"]


class Message:
    """Base representation of a Lightbus RPC/Result/Event message"""

    required_metadata: Sequence

    def __init__(self, id: str = "", native_id: str = None):
        self.id = id or str(uuid1())
        self.native_id = native_id

    def get_metadata(self) -> dict:
        """Get the non-kwarg fields of this message

        Will be used by the serializers
        """
        raise NotImplementedError()

    def get_kwargs(self) -> dict:
        """Get the kwarg fields of this message

        Will be used by the serializers
        """
        raise NotImplementedError()

    @classmethod
    def from_dict(cls, metadata: dict, kwargs: dict, **extra) -> "Message":
        """Create a message instance given the metadata and kwargs

        Will be used by the serializers
        """
        raise NotImplementedError()


class RpcMessage(Message):
    """Representation of a Lightbus RPC message"""

    required_metadata = ["id", "api_name", "procedure_name", "return_path"]

    def __init__(
        self,
        *,
        api_name: str,
        procedure_name: str,
        kwargs: Optional[dict] = None,
        return_path: Any = None,
        id: str = "",
        native_id: str = None,
    ):
        super().__init__(id, native_id)
        self.api_name = api_name
        self.procedure_name = procedure_name
        self.kwargs = kwargs
        self.return_path = return_path

    def __repr__(self):
        return "<{}: {}>".format(self.__class__.__name__, self)

    def __str__(self):
        return "{}({})".format(
            self.canonical_name, ", ".join("{}={}".format(k, v) for k, v in self.kwargs.items())
        )

    @property
    def canonical_name(self):
        return "{}.{}".format(self.api_name, self.procedure_name)

    def get_metadata(self) -> dict:
        return {
            "id": self.id,
            "api_name": self.api_name,
            "procedure_name": self.procedure_name,
            "return_path": self.return_path or "",
        }

    def get_kwargs(self):
        return self.kwargs

    @classmethod
    def from_dict(cls, metadata: Dict[str, str], kwargs: Dict[str, Any], **extra) -> "RpcMessage":
        return cls(**metadata, **extra, kwargs=kwargs)


class ResultMessage(Message):
    """Representation of a Lightbus RPC Result message"""

    required_metadata = ["id", "rpc_message_id"]

    def __init__(
        self,
        *,
        result,
        api_name: str,
        procedure_name: str,
        rpc_message_id: str,
        id: str = "",
        error: bool = False,
        trace: str = None,
        native_id: str = None,
    ):
        super().__init__(id, native_id)
        self.api_name = api_name
        self.procedure_name = procedure_name
        self.rpc_message_id = rpc_message_id

        if isinstance(result, BaseException):
            self.result = repr(result)
            self.error = True
            self.trace = "".join(
                traceback.format_exception(type(result), value=result, tb=result.__traceback__)
            )
        else:
            self.result = result
            self.error = error
            self.trace = trace

    def __repr__(self):
        if self.error:
            return "<{} (ERROR): {}>".format(self.__class__.__name__, self.result)
        else:
            return "<{} (SUCCESS): {}>".format(self.__class__.__name__, self.result)

    def __str__(self):
        return str(self.result)

    def get_metadata(self) -> dict:
        metadata = {"id": self.id, "rpc_message_id": self.rpc_message_id, "error": self.error}
        if self.error:
            metadata["trace"] = self.trace
        return metadata

    def get_kwargs(self):
        return {"result": self.result}

    @classmethod
    def from_dict(
        cls, metadata: Dict[str, str], kwargs: Dict[str, Any], **extra
    ) -> "ResultMessage":
        return cls(**metadata, **extra, result=kwargs.get("result"))


class EventMessage(Message):
    """Representation of a Lightbus Event message"""

    required_metadata = ["id", "api_name", "event_name", "version"]

    def __init__(
        self,
        *,
        api_name: str,
        event_name: str,
        kwargs: Optional[dict] = None,
        version: int = 1,
        id: str = "",
        native_id: str = None,
    ):
        super().__init__(id, native_id)
        self.api_name = api_name
        self.event_name = event_name
        self.version = int(version)
        self.kwargs = kwargs or {}

    def __repr__(self):
        return "<{}: {}>".format(self.__class__.__name__, self)

    def __str__(self):
        return "{}({})".format(
            self.canonical_name, ", ".join("{}={}".format(k, v) for k, v in self.kwargs.items())
        )

    @property
    def canonical_name(self):
        return "{}.{}".format(self.api_name, self.event_name)

    def get_metadata(self) -> dict:
        return {
            "id": self.id,
            "api_name": self.api_name,
            "event_name": self.event_name,
            "version": self.version,
        }

    def get_kwargs(self):
        return self.kwargs

    @classmethod
    def from_dict(cls, metadata: Dict[str, str], kwargs: Dict[str, Any], **extra) -> "EventMessage":
        return cls(**metadata, **extra, kwargs=kwargs)
