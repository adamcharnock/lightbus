import lightbus
from aiohttp import web

page_views = {}


def home_view(request):
    html = "<h1>Dashboard</h1>\n"
    html += "<p>Total store views</p>\n"

    html += "<ul>"
    for url, total_views in page_views.items():
        html += f"<li>URL <code>{url}</code>: {total_views} views</li>"
    html += "</ul>"

    return web.Response(body=html, content_type="text/html")


def handle_page_view(event_message, url):
    page_views.setdefault(url, 0)
    page_views[url] += 1


async def start_listener(app):
    # Create the asyncio task which will listen for the page_view event
    listener_task = await app.bus.store.page_view.listen_async(handle_page_view)
    # Store it against app in case we need it later
    app["page_view_listener"] = listener_task


async def cleanup(app):
    # We're using aiohttp to manage the event loop, so
    # we need to close up the lightbus client manually on shutdown.
    await app.bus.bus_client.close_async()


def main():
    # Make sure Lightbus formats its logs correctly
    lightbus.configure_logging()

    # Create our lightbus client and our web application
    bus = lightbus.create()
    app = web.Application()

    app.router.add_route("GET", "/", home_view)
    app.on_startup.append(start_listener)
    app.on_cleanup.append(cleanup)

    # Store the bus on `app` as we'll need it
    # in start_listener() and cleanup()
    app.bus = bus

    web.run_app(app, host="127.0.0.1", port=5000)


if __name__ == "__main__":
    main()
