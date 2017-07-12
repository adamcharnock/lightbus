from typing import Any
from bottle import HTTPResponse

class MyApi(object):
    my_event = Event()
    def method1(self: Any, user_id: int) -> dict:
        pass

    def method2(self: Any) -> HTTPResponse:
        pass


