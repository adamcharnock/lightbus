# How to combine processes

Here we will build on the dashboard we created in the [worked example] tutorial.

This *how to* requires some knowledge of Python's asyncio features.

!!! warning

    This document is out of date and needs to be updated

!!! warning

    **You can use Lightbus perfectly well without adding this complexity.**
    See the [anatomy lesson] for additional discussion.

---

Behind the scenes Lightbus is powered by Python's asyncio library. Therefore,
we can combine the Lightbus and web processes if we use an asyncio-based web server.
In this case we'll use [aiohttp] as our web server, rather than
Flask which we used earlier.

```python3
""" Lightbus & web server operating in a single process
    (dashboard/combined.py)
"""
import lightbus
from aiohttp import web

# Note we no longer need to store page view data on disk.
# We simply hold it in memory using a dictionary.
page_views = {}


def home_view(request):
    """Render the simple dashboard UI"""
    html = '<h1>Dashboard</h1>\n'
    html += '<p>Total store views</p>\n'

    html += '<ul>'
    # Read the page views from our `page_views` global variable
    for url, total_views in page_views.items():
        html += f'<li>URL <code>{url}</code>: {total_views} views</li>'
    html += '</ul>'

    return web.Response(body=html, content_type='text/html')


def handle_page_view(event_message, url):
    """Handle an incoming page view"""
    page_views.setdefault(url, 0)
    # Store the incoming view in our `page_views` global variable
    page_views[url] += 1


async def start_listener(app):
    # Create the asyncio task which will listen for the page_view event
    listener_task = await app.bus.store.page_view.listen_async(handle_page_view)

    # Store the task against `app` in case we need it later (hint: we don't)
    app['page_view_listener'] = listener_task


async def cleanup(app):
    # We're using aiohttp to manage the event loop, so
    # we need to close up the lightbus client manually on shutdown.
    # This will cancel any listeners and close down the redis connections.
    # If don't do this you'll see errors on shutdown.
    await app.bus.bus_client.close_async()


def main():
    # Make sure Lightbus formats its logs correctly
    lightbus.configure_logging()

    # Create our lightbus client and our web application
    bus = lightbus.create()
    app = web.Application()

    app.router.add_route('GET', '/', home_view)
    app.on_startup.append(start_listener)
    app.on_cleanup.append(cleanup)

    # Store the bus on `app` as we'll need it
    # in start_listener() and cleanup()
    app.bus = bus

    web.run_app(app, host='127.0.0.1', port=5000)


if __name__ == '__main__':
    main()

```

New create a new Procfile called `Procfile_combined`. This will use your new combined
dashboard process along with the existing image resizer and store services:

```bash hl_lines="5"
# Procfile_combined

image_resizer_bus: lightbus run --bus=image.bus
store_web: FLASK_DEBUG=1 FLASK_APP=store/web.py flask run --port=5001
dashboard_combined: python dashboard/combined.py
```

Note that the `dashboard_web` and `dashboard_lightbus` have gone and `dashboard_combined`
has been added.

You can now start up your new collection of services using:

```bash
$ ls
Procfile    Procfile_combined    dashboard/    image/    store/

$ honcho start -f Procfile_combined
```

[worked example]: /tutorial/worked-example.md
[aiohttp]: https://aiohttp.readthedocs.io/
[anatomy lesson]: /explanation/anatomy-lesson.md#addendum
