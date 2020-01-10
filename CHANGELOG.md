# Lightbus Changelog


## 0.10.0 - Unreleased

* The `on_error` configuration parameter has been removed. All errors are now fatal to the 
  Lightbus worker. If you wish for errors to not be fatal you should catch them in your 
  event handlers.
* Standardised on 'worker' rather than 'server' in naming:
    * Renamed `server_started`, `server_started`, and `server_ping` 
      to `worker_started`, `worker_started`, and `worker_ping`
    * Renamed `LightbusServerError` to `LightbusWorkerError`
    * Renamed `BusClient.start_server()` to `BusClient.start_worker()`
    * Renamed `BusClient.stop_server()` to `BusClient.stop_worker()`

## 0.9.1 - Unreleased

* ...

## 0.9.0 - Initial release

Changelog starts now
