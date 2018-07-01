
Lightbus' configuration happens in three stages:

1. Module loading
2. Service-level setup
3. Global bus configuration

## 1. Module loading

The first stage in Lightbus' startup is to import your `bus.py` module.
By default Lightbus will attempt to import a module named `bus`,
but you can modify this by specifying the `LIGHTBUS_MODULE`
environment variable.

This stage is only required when starting a [Lightbus process]
(i.e. `lightbus run`).

[Non-lightbus processes] will import the bus module manually in order
to access the bus client within (see next stage, below).

## 2. Service-level setup

The bus module discovered above must create a Lightbus client as
follows:

```python3
bus = lightbus.create()
```

This serves several purposes. Firstly, `lightbus run` will use this
client to access the bus. Secondly, you can import this client
elsewhere in the service in order to call RPCs and fire events.

Lastly, this allows you to configure service-level configuration
options for your lightbus client. The following options are available:

```python3
bus = lightbus.create(
    # Global bus config. Can be path to file or URL
    conifg='http://internal.mysite.com/bus/lightbus.yaml',

    # Relevent to event consumption
    service_name='my_service',

    # Will be replaced 4 random characters. Default
    process_name='{random4}',
)
```

The above configuration options can also be set using the following
environment variables:

* `LIGHTBUS_CONFIG`
* `LIGHTBUS_SERVICE_NAME`
* `LIGHTBUS_PROCESS_NAME`


### Service name

TODO

### Process name

TODO


## 3. Global bus configuration

The global bus configuration specifies the bus' overall architecture.
This takes the form of a YAML or JSON file. This file is typically
shared by all lightbus clients.

A basic default configuration is as follows:

```yaml
# Root config
bus:
  # Bus config

  schema:
    # Schema config

    transport:
      # Transport selector config

      redis:
        url: "redis://redis.svc.cluster.local:6379/0"

apis:
  # API configuration listing

  default:
    # Api config

    event_transport:
      # Transport selector config
      redis:
        url: "redis://redis.svc.cluster.local:6379/0"

    rpc_transport:
      # Transport selector config
      redis:
        url: "redis://redis.svc.cluster.local:6379/0"

    result_transport:
      # Transport selector config
      redis:
        url: "redis://redis.svc.cluster.local:6379/0"
```

Each section is detailed below.

## Root config

The available root-level configuration keys are:

* `bus` – Contains the [bus config]
* `apis` – Contains the [API configuration listing]
* `plugins` - Contains the [plugin configuration listing]

The following keys are also present, but **should
generally not be specified in your global yaml file**.
Instead they should be specified for each service, as
per the [service-level setup]:

* `service_name` – Service name
* `process_name` – Process name

## Bus config

The bus config resides under the [root config]. It contains the
following keys:

* `log_level` (default: `info`) - The log level for the `lightbus` logger. One of
  `debug`, `info`, `warning`, `error`, `critical`. `info` is a good level
  for development purposes, `warning` will be more suited to production.
* `schema` - Contains the [schema config]

## API configuration listing

The schema config resides under the [root config].

This is a key/value association between APIs and their configurations.
The reserved API name of `default` provides a catch-all configuration
for any APIs without specific configurations. Specifically configured
APIs do not inherit from the default configuration.

For example:

```yaml
...
apis:

  # Catch-all api config
  default:
      ... default config as above ...

  # Specific config for the 'marketing.analytics' API.
  # Use a different Redis instance for the high
  # volume marketing analytics
  marketing.analytics:
    # See 'API config'
    validate: false
    cast_values: false

    event_transport:
      redis:
        url: "redis://redis-marketing.svc.cluster.local:6379/0"

    rpc_transport:
      redis:
        url: "redis://redis-marketing.svc.cluster.local:6379/0"

    result_transport:
      redis:
        url: "redis://redis-marketing.svc.cluster.local:6379/0"
```

See [API config] for further details on the API options available.

## API config

The API config resides under the [API configuration listing].

Each API can be individually configured using the options below:

* `rpc_timeout` (default: `5`) – Timeout when calling RPCs on this API
* `event_listener_setup_timeout` (default: `1`) – Timeout seconds when setting up event listeners
  (only applies when using the blocking api)
* `event_fire_timeout` (default: `1`) – Timeout seconds when firing events on the bus
  (only applies when using the blocking api)
* `validate` – Contains the [api validation config]. May also be set to
  boolean `true` or `false` to blanked enable/disable.
* `strict_validation` (default: `false`) – Raise an exception if we receive a message
  from an API for which there is no schema available on the bus. If `false`
  a warning will be emitted instead.
* `event_transport` – Contains the [transport selector].
* `rpc_transport` – Contains the [transport selector].
* `result_transport`  – Contains the [transport selector].
* `cast_values` (default: `true`) – If enabled, incoming values will be best-effort
  casted based on the annotations of the RPC method signature or event listener.
  See [typing](typing.md).
* `on_error` (default: `shutdown`) – How should errors in event handlers be
  dealt with. Must be one of `ignore`, `stop_listener`, or `shutdown`.
  In all cases the exception will be logged. `ignore` will simply log the error and
  continue processing events. `stop_listener` will consume no further events
  for that listener, but other event listeners will continue as normal.
  `shutdown` will cause the Lightbus process to exit with a non-zero exit code.

## Schema config

The schema config resides under the [bus config].

* `human_readable` (default: `True`) – Should the schema JSON be transmitted
  with human-friendly indentation and spacing?
* `ttl` (default: `60`) – Integer number of seconds that an API schema should
  live on the bus. Each schema will be pinged every `ttl * 0.8` seconds
  in order to keep it alive. The bus will also check for new remote schemas
  every `ttl * 0.8` seconds.
* `transport` – Contains the schema [transport selector]

## Transport selector

The schema config resides under both the [API config] and the
[schema config].

Transports are specifed as follows:

```
... parent yaml ...
    [transport-name]:
        option: "value"
        option: "value"
```

Where `[transport-name]` can be one of:

* `redis` – The redis-backed transport.
* `debug` – A debug transport which logs what happens but
            takes no further action.
* `direct` – An experimental in-memory transport. Provides no
             network communication and will be useful for
             single process testing only.

The table below details which transports can be used in which
situations

| Transport             | RPC | Result | Event | Schema |
| --------------------- |:---:|:------:|:-----:|:------:|
| `redis`               | ✔   | ✔      | ✔     | ✔      |
| `debug`               | ✔   | ✔      | ✔     | ✔      |
| `direct`              | ✔   | ✔      | ✔     | -      |

Additional transports may be added in future. A single API
can use different types for each of its `rpc`, `result`,
and `event` needs.

The `schema` transport is global to the bus, and is not
configurable on a per-api level.

For more information see [transports](transports.md).


## API validation config

The schema config resides under the `validate` key within
the [API config].

Available options are:

* `outgoing` (default `true`) – Validate outgoing messages against any
  available API schema.
* `incoming` (default `true`) – Validate incoming messages against any
  available API schema.

A warning will be emitted if validation is enabled and the schema
is not present on the bus.
You can turn this into an error by enabling `strict_validation`
within the [API config].

[service-level setup]: #2-service-level-setup
[root config]: #root-config
[bus config]: #bus-config
[API configuration listing]: #api-configuration-listing
[plugin configuration listing]: #plugin-configuration-listing
[schema config]: #schema-config
[transport selector]: #transport-selector
[api config]: #api-config
[api validation config]: #api-validation-config
