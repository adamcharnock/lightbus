from typing import Optional, Dict, Any

from lightbus.exceptions import InvalidRpcMessage

__all__ = ['Message']


class Message(object):

    def to_dict(self) -> dict:
        raise NotImplementedError()

    @classmethod
    def from_dict(cls, dictionary: dict) -> 'Message':
        raise NotImplementedError()


class RpcMessage(Message):

    def __init__(self, *, api_name: str, procedure_name: str, kwargs: dict=Optional[None], return_path: Any=None):
        self.api_name = api_name
        self.procedure_name = procedure_name
        self.kwargs = kwargs
        self.return_path = return_path

    def __repr__(self):
        return '<{}: {}>'.format(self.__class__.__name__, self)

    def __str__(self):
        return '{}({})'.format(
            self.canonical_name,
            ', '.join('{}={}'.format(k, v) for k, v in self.kwargs.items())
        )

    @property
    def canonical_name(self):
        return "{}.{}".format(self.api_name, self.procedure_name)

    def to_dict(self) -> dict:
        dictionary = {
            'api_name': self.api_name,
            'procedure_name': self.procedure_name,
            'return_path': self.return_path or '',
        }
        dictionary.update(
            **{'kw:{}'.format(k): v for k, v in self.kwargs.items()}
        )
        return dictionary

    @classmethod
    def from_dict(cls, dictionary: Dict[str, str]) -> 'RpcMessage':
        # TODO: Consider moving this encoding/decoding logic elsewhere
        # TODO: Handle non-string types for kwargs values (schema, encoding?)
        if 'api_name' not in dictionary:
            raise InvalidRpcMessage(
                "Required key 'api_name' missing in {} data. "
                "Found keys: {}".format(cls.__name__, ', '.join(dictionary.keys()))
            )
        if 'procedure_name' not in dictionary:
            raise InvalidRpcMessage(
                "Required key 'procedure_name' missing in {} data. "
                "Found keys: {}".format(cls.__name__, ', '.join(dictionary.keys()))
            )

        api_name = dictionary.get('api_name')
        procedure_name = dictionary.get('procedure_name')

        if not api_name:
            raise InvalidRpcMessage(
                "Required key 'api_name' is present in {} data, but is empty.".format(cls.__name__)
            )
        if not procedure_name:
            raise InvalidRpcMessage(
                "Required key 'procedure_name' is present in {} data, but is empty.".format(cls.__name__)
            )

        kwargs = {k[3:]: v for k, v in dictionary.items() if k.startswith('kw:')}

        return cls(api_name=api_name, procedure_name=procedure_name, kwargs=kwargs)


class ResultMessage(Message):

    def __init__(self, *, result: str):
        # TODO: Handle different result types
        self.result = result

    def __repr__(self):
        return '<{}: {}>'.format(self.__class__.__name__, self.result)

    def __str__(self):
        return str(self.result)

    def to_dict(self) -> dict:
        return {'result': self.result}

    @classmethod
    def from_dict(cls, dictionary: dict) -> 'ResultMessage':
        if 'result' not in dictionary:
            raise InvalidRpcMessage(
                "Required key 'result' not present in message data. "
                "Found keys: {}".format(', '.join(dictionary.keys()))
            )
        result = dictionary.get('result')
        if not result:
            raise InvalidRpcMessage("Required key 'result' is present in message data, but is empty")

        return cls(result=result)


class EventMessage(Message):
    pass
