import traceback
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
        # TODO: Let's face it, this can all be neatened up quite a lot
        for required_key in ('api_name', 'procedure_name'):
            if required_key not in dictionary:
                raise InvalidRpcMessage(
                    "Required key {} missing in RpcMessage data. "
                    "Found keys: {}".format(required_key, ', '.join(dictionary.keys()))
                )

        api_name = dictionary.get('api_name')
        procedure_name = dictionary.get('procedure_name')
        return_path = dictionary.get('return_path')

        if not api_name:
            raise InvalidRpcMessage(
                "Required key 'api_name' is present in {} data, but is empty.".format(cls.__name__)
            )
        if not procedure_name:
            raise InvalidRpcMessage(
                "Required key 'procedure_name' is present in {} data, but is empty.".format(cls.__name__)
            )

        kwargs = {k[3:]: v for k, v in dictionary.items() if k.startswith('kw:')}

        return cls(api_name=api_name, procedure_name=procedure_name, return_path=return_path, kwargs=kwargs)


class ResultMessage(Message):

    def __init__(self, *, result):
        self.result = result
        self.error = isinstance(result, BaseException)

    def __repr__(self):
        if self.error:
            return '<{} (ERROR): {}>'.format(self.__class__.__name__, self.result)
        else:
            return '<{} (SUCCESS): {}>'.format(self.__class__.__name__, self.result)

    def __str__(self):
        return str(self.result)

    def to_dict(self) -> dict:
        if self.error:
            trace = ''.join(traceback.format_exception(
                etype=type(self.result),
                value=self.result,
                tb=self.result.__traceback__
            ))
            return {
                'result': str(self.result),
                'error': True,
                'trace': trace
            }
        else:
            return {
                'result': self.result,
                'error': False
            }

    @classmethod
    def from_dict(cls, dictionary: dict) -> 'ResultMessage':
        if 'result' not in dictionary:
            raise InvalidRpcMessage(
                "Required key 'result' not present in ResultMessage data. "
                "Found keys: {}".format(', '.join(dictionary.keys()))
            )
        result = dictionary.get('result')
        if not result:
            raise InvalidRpcMessage("Required key 'result' is present in message data, but is empty")

        return cls(result=result)


class EventMessage(Message):

    def __init__(self, *, api_name: str, event_name: str, kwargs: dict=Optional[None]):
        self.api_name = api_name
        self.event_name = event_name
        self.kwargs = kwargs or {}

    def __repr__(self):
        return '<{}: {}>'.format(self.__class__.__name__, self)

    def __str__(self):
        return '{}({})'.format(
            self.canonical_name,
            ', '.join('{}={}'.format(k, v) for k, v in self.kwargs.items())
        )

    @property
    def canonical_name(self):
        return "{}.{}".format(self.api_name, self.event_name)

    def to_dict(self) -> dict:
        dictionary = {
            'api_name': self.api_name,
            'event_name': self.event_name,
        }
        dictionary.update(
            **{'kw:{}'.format(k): v for k, v in self.kwargs.items()}
        )
        return dictionary

    @classmethod
    def from_dict(cls, dictionary: dict):
        # TODO: This has a lot in common with RpcMessage, consider refactoring
        #       *IF* it will reduce complexity.
        for required_key in ('api_name', 'event_name'):
            if required_key not in dictionary:
                raise InvalidRpcMessage(
                    "Required key {} missing in RpcMessage data. "
                    "Found keys: {}".format(required_key, ', '.join(dictionary.keys()))
                )

        api_name = dictionary.get('api_name')
        event_name = dictionary.get('event_name')

        if not api_name:
            raise InvalidRpcMessage(
                "Required key 'api_name' is present in {} data, but is empty.".format(cls.__name__)
            )
        if not event_name:
            raise InvalidRpcMessage(
                "Required key 'event_name' is present in {} data, but is empty.".format(cls.__name__)
            )

        kwargs = {k[3:]: v for k, v in dictionary.items() if k.startswith('kw:')}

        return cls(api_name=api_name, event_name=event_name, kwargs=kwargs)
