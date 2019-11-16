import os
from datetime import datetime

import django
import lightbus
from lightbus.utilities.django import uses_django_db

bus = lightbus.create()


class AnalyticsApi(lightbus.Api):
    page_view = lightbus.Event(
        parameters=(
            lightbus.Parameter("pk", int),
            lightbus.Parameter("viewed_at", datetime),
            lightbus.Parameter("url", str),
            lightbus.Parameter("user_agent", str),
        )
    )

    @uses_django_db
    def get_total(self, url: str) -> int:
        from lightbus_examples.ex06_django.example_app.models import PageView

        return PageView.objects.filter(url=url).count()

    class Meta:
        name = "analytics"


# Tell the client to respond to this API
bus.client.register_api(AnalyticsApi())


@bus.client.on_start()
async def bus_start(**kwargs):
    # Setup the default DJANGO_SETTINGS_MODULE
    # (as we also do in manage.py and wsgi.py)
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "lightbus_examples.ex06_django.settings")

    # Sets up django. We must do this before importing any models
    django.setup()
