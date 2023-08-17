# Lightbus Changelog

## 1.3.0

* Enhancement: Python 3.10 & 3.11 now supported
* Vendored aioredis due to compatibility issues
* Breaking: Python 3.7 & 3.8 no longer supported

## 1.2.0

* Updating @uses_django_db helper to use django's built-in db connection cleaning up facilities. 
  This should more closely mirror Django's own internal request-based connection management
* Errors are now reported in a single log message. This should make error logging much more sane.
* Human name is now generated using the `random` builtin rather than `secret`, as this does not need to be crypt-secure
* Event firing sanity checking now correctly respects non-required parameters

## 1.1.1

* Now correctly creating schemas from custom classes with `@property` decorated methods

## 1.1.0

* Enhancement: Python 3.9 now supported

## 1.0.6

* Bug: Now showing the welcome message only once
* Enhancement: `TransportPool` now has a sensible `__str__()` implementation, which improves logging
* Bug: Prioritising use of the `__to_bus__()` method in schema creation. This ensures that
  schema generation and data deforming are now consistent

## 1.0.5

* Enhancement: An `EventMessage` is now returned when firing any event.

## 1.0.4

* Bug: `__from_bus__()` will now correctly operate upon non-mapping types

## 1.0.3

* Enhancement: Smarter schema generation for objects with` __to_bus__()` methods. 
  The return annotation of the `__to_bus__()` method will now be used for schema generation
* Bug: Fixing type hint on `cast_to_hint()` method definition
* Bug: Adjusting `inspect` command to ensure using the experimental option `--show-casting` doesn't 
  cause the whole command to explode

## 1.0.2

* Enhancement: Dates & datetimes will now be parsed from their string format using `dateutil` where available.
               This provides more flexible parsing of date formats.

## 1.0.1

* Bug: Scheduled tasks were not been executed as expected

## 1.0.0

* No changes since `0.11.0`, appears to be stable

## 0.11.0

* Lightbus should now behave sensibly in threaded environments

## 0.10.0

* Major refactoring to Lightbus' internals. Resolves a number of long standing issues.
  Lightbus now communicates internally using commands sent via queues.
* Python 3.8 now supported
* The `on_error` configuration parameter has been removed and replaced with an 
  `on_error` argument to `listen()`. [See docs](https://lightbus.org/reference/events/#errors-in-event-listeners)
* Standardised on 'worker' rather than 'server' in naming:
    * Renamed `server_started`, `server_started`, and `server_ping` 
      to `worker_started`, `worker_started`, and `worker_ping`
    * Renamed `LightbusServerError` to `LightbusWorkerError`
    * Renamed `BusClient.start_server()` to `BusClient.start_worker()`
    * Renamed `BusClient.stop_server()` to `BusClient.stop_worker()`
* More cases are now handled in Redis connection retrying (connection refused and Redis in LOADING state).
  These would previously result in an unhandled exception. 
* Added experimental `--validate` and `--show-casting` arguments to 
  [lightbus inspect](https://lightbus.org/reference/command-line-use/inspect/)

## 0.9.0 - Initial release

Changelog starts now
