import asyncio


import lightbus
from lightbus.utilities.async import cancel
from aiohttp import web

page_views = {}


def home_view(request):
    html = '<h1>Dashboard</h1>\n'
    html += '<p>Total store views</p>\n'

    html += '<ul>'
    for url, total_views in page_views.items():
        html += f'<li>URL <code>{url}</code>: {total_views} views</li>'
    html += '</ul>'

    return web.Response(body=html, content_type='text/html')


def handle_page_view(url):
    page_views.setdefault(url, 0)
    page_views[url] += 1


async def start_listener(app):
    app['page_view_listener'] = await app.bus.store.page_view.listen_async(handle_page_view)


async def cleanup_listener(app):
    await cancel(app['page_view_listener'], *asyncio.Task.all_tasks())


async def make_app(bus, loop):
    app = web.Application()

    app.router.add_route('GET', '/', home_view)

    app.bus = bus
    app.on_startup.append(start_listener)
    app.on_cleanup.append(cleanup_listener)
    return app


def main():
    loop = asyncio.get_event_loop()
    lightbus.configure_logging()
    bus = lightbus.create(loop=loop)

    app = loop.run_until_complete(make_app(bus, loop))
    web.run_app(app, host='127.0.0.1', port=5000)


if __name__ == '__main__':
    main()
