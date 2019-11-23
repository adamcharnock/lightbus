# Transport configuration

Lightbus ships with built-in support for Redis. This is provided by the following transports:

* An event transport – sends and consumes RPC calls
* An RPC transport – sends and receives RPC results
* A result transport – sends and consumes events
* A schema transport – stores and retrieves the [bus schema](../explanation/schema.md)

{!docs/includes/note-configuration-auto-complete.md!}

## Complete configuration example

```yaml
...
apis:

  # Catch-all api config
  default:
    # ...API Config options here (see configuration reference)...
    
    # Per-transport configuration
    event_transport:
      redis:
        url: "redis://redis_host:6379/0"
        batch_size: 10
        reclaim_batch_size: 100
        serializer: "lightbus.serializers.ByFieldMessageSerializer"
        deserializer: "lightbus.serializers.ByFieldMessageDeserializer"
        acknowledgement_timeout: 60
        max_stream_length: 100000
        stream_use: "per_api"
        consumption_restart_delay: 5
        consumer_ttl: 2592000

    rpc_transport:
      redis:
        url: "redis://redis_host:6379/0"
        batch_size: 10
        serializer: "lightbus.serializers.BlobMessageSerializer"
        deserializer: "lightbus.serializers.BlobMessageDeserializer"
        rpc_timeout: 5
        rpc_retry_delay: 1
        consumption_restart_delay: 5

    result_transport:
      redis:
        url: "redis://redis_host:6379/0"
        serializer: "lightbus.serializers.BlobMessageSerializer"
        deserializer: "lightbus.serializers.BlobMessageDeserializer" 
        rpc_timeout: 5 
        rpc_retry_delay: 1 
        consumption_restart_delay: 5 
  
  # Can respecify the above config for specific APIs 
  # if customisation is needed.
  marketing.analytics:
    ...

# Schema transport configuration is at the root level
schema:
    transport:
      redis:
        url: "redis://redis.svc.cluster.local:6379/0"
```

## Redis Event Transport configuration

### `url`

*Type: `str`, default: `redis://127.0.0.1:6379/0`*

The connection string for the redis server. Format is:

    `redis://host:port/db_number`


### `batch_size`

*Type: `int`, default: `10`* 

The maximum number of messages to be fetched at one time. A higher value will reduce overhead 
for large volumes of messages. 

However, should a worker die then the processing of the fetched 
messages will be delayed by `acknowledgement_timeout`. In this case those messages will be 
processed out-of-order.

### `reclaim_batch_size`

*Type: `int`, default: `reclaim_batch_size * 10`* 

The maximum number of messages to be fetched at one time *when reclaiming timed out messages*.

### `serializer`

*Type: `str`, default: `lightbus.serializers.ByFieldMessageSerializer`* 

The serializer to be used in converting the lightbus message into a bus-appropriate format.

### `deserializer`

*Type: `str`, default: `lightbus.serializers.ByFieldMessageDeserializer`* 

The deserializer to be used in converting the lightbus message into a bus-appropriate format.

### `acknowledgement_timeout`

*Type: `float`, default: `60.0`, seconds* 

Any message not processed `acknowledgement_timeout` seconds will assume to have failed and 
will therefore be picked up by another Lightbus worker. This is typically caused by a Lightbus worker 
exiting ungracefully.

!!! important "Long running events"
    
    You will need to modify this if you have event handlers which take a long time to execute. 
    This value must exceed the length of time it takes any event to be processed

### `max_stream_length`

*Type: `int`, default: `100_000`* 

Streams will be trimmed so they never exceed the given length. Set to `null` for no limit.

### `stream_use`

*Type: `str`, default: `per_api`* 

How should Redis streams be created? There are two options:

* `per_api` – One stream for an entire API.
* `per_event` – One stream for each event on an API

Setting this to `per_api` will ensure event listeners receive all events on an api in order. 
However, all events for the API will be received even if not needed (Lightbus will discard these 
unwanted events before passing them to your event handler). This will consume unnecessary resources 
if an API contains high-volume events which your listener does not care for.

Setting this to `per_event` will ensure that Lightbus only receives the needed events, but 
messages will only be ordered for an individual event.

### `consumption_restart_delay`

*Type: `int`, default: `5`, seconds* 

How long to wait before attempting to reconnect after loosing the connection to Redis.

### `consumer_ttl`

*Type: `int`, default: `2_592_000`, seconds* 

How long to wait before cleaning up inactive consumers. Default is 30 days.

## Redis RPC Transport configuration

### `url`

*Type: `str`, default: `redis://127.0.0.1:6379/0`*

The connection string for the redis server. Format is:

    `redis://host:port/db_number`

### `batch_size`

*Type: `int`, default: `10`* 

The maximum number of messages to be fetched at one time. A higher value will reduce overhead 
for large volumes of messages. 

### `serializer`

*Type: `str`, default: `lightbus.serializers.BlobMessageSerializer`* 

The serializer to be used in converting the lightbus message into a bus-appropriate format.

### `deserializer`

*Type: `str`, default: `lightbus.serializers.BlobMessageDeserializer`* 

The deserializer to be used in converting the lightbus message into a bus-appropriate format.

### `rpc_timeout`

*Type: `int`, default: `5`, seconds*

How long to wait before we give up waiting for an RPC to be processed.

!!! note "Note on `rpc_timeout`"
    
    This configuration option is also repeated in the [API config]. For now 
    this value needs to be specified in three places:
        
      * The [API config]    
      * RPC transport config
      * Result transport config (see below)

### `rpc_retry_delay`

*Type: `int`, default: `1`, seconds* 

How long to wait before reattempting to call the remote RPC. For example, 
in cases where Redis errors (e.g. connection issues). Execution will be retried once only.

### `consumption_restart_delay`

*Type: `int`, default: `5`, seconds* 

How long to wait before attempting to reconnect after loosing the connection to Redis.

## Redis Result Transport configuration


### `url`

*Type: `str`, default: `redis://127.0.0.1:6379/0`*

The connection string for the redis server. Format is:

    `redis://host:port/db_number`

### `serializer`

*Type: `str`, default: `lightbus.serializers.BlobMessageSerializer`* 

The serializer to be used in converting the lightbus message into a bus-appropriate format.

### `deserializer`

*Type: `str`, default: `lightbus.serializers.BlobMessageDeserializer`* 

The deserializer to be used in converting the lightbus message into a bus-appropriate format.

### `rpc_timeout`

*Type: `int`, default: `5`, seconds*

How long to wait before we give up waiting for an RPC result to be received.

!!! note "Note on `rpc_timeout`"
    
    This configuration option is also repeated in the [API config]. For now 
    this value needs to be specified in three places:
        
      # The [API config]    
      * RPC transport config (see above)
      * Result transport config

### `rpc_retry_delay`

*Type: `int`, default: `1`, seconds* 

How long to wait before reattempting to call the remote RPC. For example, 
in cases where Redis errors (e.g. connection issues). Execution will be retried once only.

### `consumption_restart_delay`

*Type: `int`, default: `5`, seconds* 

How long to wait before attempting to reconnect after loosing the connection to Redis.

## Redis Schema Transport configuration

### `url`

*Type: `str`, default: `redis://127.0.0.1:6379/0`*

The connection string for the redis server. Format is:

    `redis://host:port/db_number`


[API config]: configuration.md#api-config
