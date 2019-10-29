# Services

A *service* is one or more processes handling a common task.
These processes operate as a tightly-coupled whole.

All processes in a service will generally:

* Share the same API class definitions
* Moreover, they will normally share the same codebase
* Create a single instance of the bus client in `bus.py` using
  `bus = lightbus.create()`.

---

For example, your company may have the following:

* An online store
* A price monitoring script
* An image resizing resizing process 

Each of these would be a service.

The store service would have a web process and a
Lightbus process. The image resizing service & and price monitoring services 
would each likely have a Lightbus process only.

A simple lightbus deployment could therefore look something like this:

![A simple Lightbus deployment][simple-processes]

[simple-processes]: /static/images/simple-processes.png

In this example the following actions would take place:

* Django reads from the web service database in order to serve web content
* The online shop's Lightbus process receives pricing events from the
  price monitoring service. It updates products in the database using
  this new pricing data.
* When the Django app receives an image upload, it performs a RPC to the
  image resizing service to resize the image[^1].



[^1]: Making the Django process wait for an RPC to respond is
      probably a bad idea in this case, but it illustrates how it
      *could* be done. Using an event (which is fire-and-forget)
      could be more suitable in reality.
