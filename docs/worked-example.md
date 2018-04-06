In the following worked example we will create three services:
 
1. An image resizing service
2. An online store
3. A stats dashboard

This will involve a combination of web interfaces (using [Flask]), and Lightbus APIs. 
The goal is to show how Lightbus can allow multiple services to interact.

## Getting started

The code created here can be found in Lightbus example [ex03_worked_example], although 
the code will be repeated below. There is a directory for each service we will 
create – `store/`, `dashboard/`, and `image/`.

Before continuing ensure you have installed `flask` and `honcho`:

    pip3 install flask honcho

A passing familiarity with [Flask] may be useful, but is not required. [Honcho] 
will assist us in running the various processes required for our services. 

We will assume you 
have already read and completed the [installation](installation.md), 
[quick start](quick-start.md), and [concepts](concepts.md) sections.

## Image resizing service

The image resizing service will be a simple Lightbus API, the purpose of which 
is to allow our store to resize images prior to display:

```python3
# image/bus.py
from lightbus import Api, Event

class ImageApi(Api):

    class Meta:
        name = 'image'

    def resize(self, url, width, height):
        # This is a demo, so just return an animal picture of the correct size
        return f'https://placeimg.com/{width}/{height}/animals?_={url}'
```

There is no web interface for this service, so this is all we need.

## Store service

Our store will have both a Lightbus API and a web interface. We'll start 
with the API first:

```python3
# store/bus.py
from lightbus import Api, Event, configure_logging
configure_logging()

class StoreApi(Api):
    page_view = Event(parameters=('url', ))

    class Meta:
        name = 'store'
```

This API has a single event called ``page_view``. The store web interface will fire this 
event whenever a page is viewed.

Our store web interface uses Flask and is a little longer:

```python3
import lightbus
from flask import Flask

# Ensure the Store API is loaded so we can fire events on it
from .bus import StoreApi

# Setup flask
app = Flask(__name__)

# Create our bus
bus = lightbus.create()

# A dummy list of pets our store will sell
PETS = (
    'http://store.company.com/image1.jpg',
    'http://store.company.com/image2.jpg',
    'http://store.company.com/image3.jpg',
)

@app.route('/')
def home():
    # A view to list all available pets
    html = '<h1>Online pet store</h1><br>'

    for pet_num, image_url in enumerate(PETS):
        # Get an image of the appropriate size
        resized_url = bus.image.resize(url=image_url, width=200, height=200)
        html += (
            f'<a href="/pet/{pet_num}">'
            f'<img src="{resized_url}">'
            f'</a> '
        )
    
    # Fire the page view
    bus.store.page_view.fire(url='/')
    
    return html

@app.route('/pet/<int:pet_num>')
def pet(pet_num):
    # Show an individual pet
    resized_url = bus.image.resize(url=PETS[pet_num], width=200, height=200)
    
    # Fire the page view
    bus.store.page_view.fire(url=f'/pet/{pet_num}')

    html = f'<h1>Pet {pet_num}</h1>'
    html = f'<img src="{resized_url}"><br />'
    return html
```

## Interlude: give it a go

We're not quite done yet, but you can now startup the necessary processes and 
see the store. You will need to run each of these in a separate terminal window:
    
```shell
$ ls
image/       store/

# Start our image resizing service
$ lightbus run --bus=image/bus.py

# Start our store's web interface
$ FLASK_APP=store/web.py flask run --port=5001
```

Now open [127.0.0.1:5001] in your browser and 
you should see three animal pictures awaiting you. The URL for each 
image was fetched from the image resizing service.

The flask web interface should also have some logging output akin to the following:

![flask web app logging output][web-log]

Here you can see:

1. `image.resize` was called three times, once for each image
2. The `store.page_view` event was fired

Next we will create the dashboard which will make use of the `store.page_view` event.

## Dashboard service

The dashboard service will provide internal reporting in the form 
of page view statistics for the online store.

There dashboard will need to both receive events and provide a web 
interface. It will therefore need both a lightbus process and a 
web process (we will later look at how to combine these).

Fist we will start with the `bus.py` file:

```python3
# dashboard/bus.py
import json

page_views = {}

def handle_page_view(api_name, event_name, url):
    page_views.setdefault(url, 0)
    page_views[url] += 1
    with open('/tmp/.dashboard.db.json', 'w') as f:
        json.dump(page_views, f)

def before_server_start(bus):
    # Called when lightbus starts up
    bus.store.page_view.listen(handle_page_view)
```

This is a simple listener for the `bus.store.page_view` event. This is event is fired by the 
store's web interface we created above.

Note we do not define any APIs, 
instead we setup our event listener using the `before_server_start()` hook. Listening for this 
event is all the dashboard's Lightbus process will do, it will not provide any APIs.

The `handle_page_view()` handler persists each view to the Dashboard services' local database. 
In a real service this would likely be a DBMS of some form (Postgres, MySQL, 
Redis, Mongo etc). For simplicity we just store JSON to a file.

Now we'll define our dashboard's web interface:

```python3
# dashboard/web.py
import json
from flask import Flask

app = Flask(__name__)

@app.route('/')
def home():
    html = '<h1>Dashboard</h1>\n'
    html = '<p>Total store views</p>\n'

    with open('/tmp/.dashboard.db.json', 'r') as f:
        page_views = json.load(f)

    html += '<ul>'
    for url, total_views in page_views.items():
        html += f'<li>URL <code>{url}</code>: {total_views}</li>'
    html += '</ul>'

    return html
```

This reads the JSON data that was written by the event listener in `dashboard/bus.py` above, 
then render it to HTML.

## Run it!

You should now have the following python files:

    image/bus.py
    store/bus.py
    store/web.py
    dashboard/bus.py
    dashboard/web.py

This translates into the following processes:

| Service        | Process type  | Purpose                                                       |
| -------------- | ------------- | ------------------------------------------------------------- |
| Image reszier  | Lightbus      | Will resize images and return a new URL                       |
| Store          | Web           | Render the store UI. Use bus to resize image and fire events  |
| ~~Store~~      | ~~Bus~~       | While the store does have a `bus.py`, it does not have any RPCs to serve. We therefore do not need to run a lightbus service. |
| Dashboard      | Web           | Render the dashboard web UI, read data from database          |
| Dashboard      | Lightbus      | Listen for page view events and store stats to database       |

You can run each of these as follows:

```shell
$ ls
dashboard/    image/    store/

# Image resizer
$ lightbus run --bus=image/bus.py

# Store
$ FLASK_APP=store/web.py flask run --port=5001

# Dashboard
$ lightbus run --bus=dashboard/bus.py
$ FLASK_APP=dashboard/web.py flask run --port=5000
```

**However,** you may find it easier to startup these processes 
will the `honcho` tool we installed earlier. First, create 
a file called `Procfile`:

```shell
# Procfile
image_resizer_bus: lightbus run --bus=image/bus.py
store_web:         FLASK_APP=store/web.py flask run --port=5001
dashboard_bus:     lightbus run --bus=dashboard/bus.py
dashboard_web:     FLASK_APP=dashboard/web.py flask run --port=5000
```

And now use `honcho` to startup all the processes together:

```shell
$ ls
Procfile    dashboard/    image/    store/

$ honcho start
```

If you see an error stating `command not found`, ensure you installed `honcho` as 
detailed above (`pip3 install honcho`).

Once started, checkout the output for any errors. Each log line will state the process
it came from. If all is well, you should see something like this:

![honcho startup logging output][honcho-startup]

You should now be able to access the store's web interface at [127.0.0.1:5001] as you 
did previously.

Upon viewing the page, the web interface will resize each image and fire the `store.page_view`
event. The dashboard will receive the `store.page_view` event and create the 
database for the first time. The logging output should reflect this:

![honcho page view logging output][honcho-page-view]

At this point you can view the dashboard at [127.0.0.1:5000]. Note that opening the dashboard 
before this point would have resulted in an error as the database would not have been 
created.

The dashboard should show a simple list of URLs plus the total number of page views for each.
Go back to the store and view a few pages. Now refresh the dashboard and note the new data.

## Wrapping up

While the services we have have created here are very crude, hopefully they have helped 
show how Lightbus can be used as a effective communications infrastructure.

Next we will the detail the ins-and-outs of each area of Lightbus – APIs, RPCs, events, configuration, 
schemas, and so on.

## Extra: Combining dashboard processes

As an extra bonus, below we show how to combine both dashboard processes into one.
This is a bit more advanced and requires some knowledge of Python's asyncio features.
*You can use Lightbus perfectly well without adding this complexity.*

Behind the scenes Lightbus is powered by Python's asyncio library. Therefore, 
if we also use an asyncio-based web server we can combine the Lightbus and web 
processes. In this case we'll use [aiohttp] as our web server, rather than 
Flask which we used earlier.


```python3
# dashboard/combined.py
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


def handle_page_view(url):
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

image_resizer_bus: lightbus run --bus=image/bus.py
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

[Flask]: http://flask.pocoo.org/
[ex03_worked_example]: https://github.com/adamcharnock/lightbus/tree/master/lightbus_examples/ex03_worked_example
[Procfile]: https://github.com/adamcharnock/lightbus/blob/mater/lightbus_examples/ex03_worked_example/Procfile
[web-log]: static/images/worked-example-flask-log.png
[honcho-startup]: static/images/worked-example-honcho-startup.png
[honcho-page-view]: static/images/worked-example-honcho-page-view.png
[127.0.0.1:5001]: http://127.0.0.1:5001
[127.0.0.1:5000]: http://127.0.0.1:5000
[aiohttp]: https://aiohttp.readthedocs.io/
