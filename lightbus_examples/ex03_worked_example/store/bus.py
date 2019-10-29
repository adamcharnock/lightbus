# store/bus.py
import lightbus
import threading

print(threading.current_thread())

bus = lightbus.create(flask=True)


class StoreApi(lightbus.Api):
    page_view = lightbus.Event(parameters=("url",))

    class Meta:
        name = "store"


bus.client.register_api(StoreApi())
