"""
This bus.py file listens for events only and does not
provide any APIs. It receives page view events and
writes the data to .exampledb.json.

"""
import json
import lightbus
import lightbus.creation

bus = lightbus.creation.create()
page_views = {}


def handle_page_view(event_message, url):
    page_views.setdefault(url, 0)
    page_views[url] += 1
    with open("/tmp/.dashboard.db.json", "w") as f:
        json.dump(page_views, f)


def before_server_start():
    bus.store.page_view.listen(handle_page_view)
