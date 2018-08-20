# Anatomy lesson

When using Lightbus you will still run your various services
as normal. For web-based software this will likely include one or more
processes to handle web traffic (e.g. Django, Flask).
You may or may not also have some other processes running for other purposes.

In addition to this, Lightbus will have its own process started via
`lightbus run`.

While the roles of these processes are not strictly defined, in most
circumstances their use should break down as follows:

* **Lightbus processes** – Respond to remote procedure calls, listen for
  and handle events.
* **Other processes (web etc)** – Perform remote procedure calls, fire events

The starting point for the lightbus process is a `bus.py` file. You
should create this in your project root. You can also configure
where Lightbus looks for this module using the `--bus` option or
by setting the `LIGHTBUS_MODULE` environment variable.
