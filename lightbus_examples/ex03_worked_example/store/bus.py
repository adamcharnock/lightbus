# store/bus.py
import lightbus

bus = lightbus.create(flask=True)


class StoreApi(lightbus.Api):
    page_view = lightbus.Event(parameters=("url",))

    class Meta:
        name = "store"
