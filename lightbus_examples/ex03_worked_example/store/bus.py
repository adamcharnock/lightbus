# store/bus.py
from lightbus import Api, Event, create

bus = create(flask=True)


class StoreApi(Api):
    page_view = Event(parameters=("url",))

    class Meta:
        name = "store"
