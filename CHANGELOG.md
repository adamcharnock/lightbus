# Lightbus Changelog


## 0.10.0 - Unreleased

* Python 3.8 now supported
* The `on_error` configuration parameter has been removed and replaced with an 
  `on_error` argument to `listen()`. [See docs](https://lightbus.org/reference/events/#errors-in-event-listeners)
* Standardised on 'worker' rather than 'server' in naming:
    * Renamed `server_started`, `server_started`, and `server_ping` 
      to `worker_started`, `worker_started`, and `worker_ping`
    * Renamed `LightbusServerError` to `LightbusWorkerError`
    * Renamed `BusClient.start_server()` to `BusClient.start_worker()`
    * Renamed `BusClient.stop_server()` to `BusClient.stop_worker()`
* More cases are now handled in Redis connection retrying (connection refused and redis in LOADING state).
  These would previously result in an unhandled exception. 
* Added experimental `--validate` and `--show-casting` arguments to 
  [lightbus inspect](https://lightbus.org/reference/command-line-use/inspect/)

## 0.9.0 - Initial release

Changelog starts now
