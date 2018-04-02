# store/bus.py
from lightbus import Api, Event, configure_logging
configure_logging()

class StoreApi(Api):
    page_view = Event(parameters=('url', ))

    class Meta:
        name = 'store'
