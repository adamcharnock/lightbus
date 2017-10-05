from typing import Optional

__all__ = ['Message']


class Message(object):
    __slots__ = ['body', 'headers']

    def __init__(self, body: bytes=b'', headers: dict=Optional[None]):
        self.body = body
        self.headers = headers if headers is not None else {}
