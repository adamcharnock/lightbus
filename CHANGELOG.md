# Lightbus Changelog

## 1.0.5

* An `EventMessage` is now returned when firing any event.

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
