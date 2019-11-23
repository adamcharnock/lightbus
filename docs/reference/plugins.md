Lightbus ships with two plugins, both of which are disabled by default.

## State Plugin

Every Lightbus worker process which has the state plugin enabled will report it's state 
to the bus. This state information is available as the following events on the 
`internal.state` API.

This plugin should add minimal load to the bus and may be useful in developing 
tooling around the bus.

### Events

#### `server_started`

Parameters: `process_name`, `metrics_enabled`, `api_names`, `listening_for`, `timestamp`, `ping_interval`

Fired when the worker starts up.

#### `server_ping`

Parameters: `process_name`, `metrics_enabled`, `api_names`, `listening_for`, `timestamp`, `ping_interval`

Fires every 60 seconds after worker startup. This indicates that the worker is still alive and has not 
died unexpectedly. This interval is configurable (see below).

#### `server_stopped`

Parameters: `process_name`, `timestamp`

Fires when a worker shuts down cleanly.

### Configuration

The following configuration options are available:

#### `enabled` (bool)

Default: `False`

Should the plugin be enabled?

#### `ping_enabled` (bool)

Default: `True`

Should ping messages be sent?

#### `ping_enabled` (int, seconds)

Default: `60`

How often (in seconds) should a ping event be sent. A lower interval means more frequent messages, but reduces the 
time it takes any listeners to discover dead workers.

#### Example configuration

```yaml
bus:
  ...

apis:
  ...

plugins:
  internal_state:
    enabled: true
    ping_enabled: true
    ping_interval: 60
```

## Metrics Plugin

The metrics plugin sends metric events for every event & RPC processes. It therefore has a much 
bigger impact on performance than the state plugin, but also provides much more detailed information.

### Events

The following events will be fired on the `internal.metrics` API:

| Event | Parameters |
| --- | --- |
| `rpc_call_sent` | `process_name`, `id`, `api_name`, `procedure_name`, `kwargs`, `timestamp` |
| `rpc_call_received` | `process_name`, `id`, `api_name`, `procedure_name`, `timestamp` |
| `rpc_response_sent` | `process_name`, `id`, `api_name`, `procedure_name`, `result`, `timestamp` |
| `rpc_response_received` | `process_name`, `id`, `api_name`, `procedure_name`, `timestamp` |
| `event_fired` | `process_name`, `event_id`, `api_name`, `event_name`, `kwargs`, `timestamp` |
| `event_received` | `process_name`, `event_id`, `api_name`, `event_name`, `kwargs`, `timestamp` |
| `event_processed` | `process_name`, `event_id`, `api_name`, `event_name`, `kwargs`, `timestamp` |

### Configuration

The metrics plugin only includes the `enabled` configuration option. Configuration should therefore be:

```yaml
bus:
  ...

apis:
  ...

plugins:
  internal_metrics:
    enabled: true
```
