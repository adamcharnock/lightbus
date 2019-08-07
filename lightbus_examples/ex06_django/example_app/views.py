from django.http import HttpResponse
from django.shortcuts import render

from lightbus.exceptions import LightbusTimeout
from lightbus_examples.ex06_django.bus import bus
from lightbus_examples.ex06_django.example_app.models import PageView


def home_page(request):
    current_url = request.META.get("PATH_INFO", "")

    PageView.objects.create(url=current_url, user_agent=request.META.get("HTTP_USER_AGENT", ""))

    html = (
        "<h1>Welcome!</h1>\n"
        "<p>This is the home page. It's nothing special, but we have logged your page view. "
        "Thank you for visiting!</p>\n"
    )

    html += "<h2>Total views</h2>"

    try:
        total_views = bus.analytics.get_total(url=current_url)
        html += f"<p>There have been {total_views} views for this page</p>"
    except LightbusTimeout:
        html += (
            f"<p>The bus did not respond in a timely fashion. Have you started the "
            f"Lightbus worker process using <code>lightbus run</code>?</p>"
        )

    return HttpResponse(html)
