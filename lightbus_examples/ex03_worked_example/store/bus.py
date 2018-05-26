# store/bus.py
from lightbus import Api, Event


class StoreApi(Api):
    page_view = Event(parameters=("url",))

    class Meta:
        name = "store"
