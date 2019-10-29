# Event protocol (Redis)

Here we document the specific interactions between Lightbus and Redis. 
The concrete implementation of this is provided by the `RedisEventTransport` class. 
Before reading you should be familiar with Lightbus' [data marshalling].

This documentation may be useful when debugging, developing third-party client libraries, 
or simply for general interest and review. **You do not need to be aware of this 
protocol in order to use Lightbus**.

## Sending events

The following commands will send an event:

```
XADD {stream_name} [MAXLEN ~ {max_stream_length}] * {field_name_1} {field_value_1} {field_name_2} {field_value_2}...
```

### `{stream_name}`

The `{stream_name}` value is composed in one of the following ways:

* One stream per event: `{api_name}.{event_name}`
* One stream per API: `{api_name}.*`

### `MAXLEN`

The `MAXLEN ~ {max_stream_length}` component is optional, but will be used by Lightbus to 
limit the stream to the approximate configured length. 

### Fields

Field names are strings. Field values are JSON-encoded strings.

The following metadata fields must be sent:

* `:id` - A unique message ID
* `:api_name` - The API name for this event
* `:event_name` - The name of this event
* `:version` – The version of this event (`1` is a sensible default value)

Note that metadata fields are prefixed by a colon. 
User-specified fields should not include this colon.

Lightbus does not currently provide specific functionality around the `version` 
field, but the field is available to developers via the `EventMessage` class. 
Lightbus may implement event functionality around event versions in future 
(such as event migrations).


## Consuming events

Consuming events involves an initial setup stage in which we check for any 
events which this process has consumed yet failed to process (for example, 
due to an error, hardware failure, network problem, etc).

We perform this initial check for events as follows:

```
XREAD_GROUP {group_name} {consumer_name} {stream_name} 0
```

The `0` above indicates we wish to receive un-acknowledged events for this consumer 
(i.e this Lightbus process).

Once we have received and processed any of these events, we can retrieve further events as follows:

```
XREAD_GROUP {group_name} {consumer_name} {stream} >
```

### `{group_name}`

The `{group_name}` value is comprised of the [service name and listener name] as follows: 
`{service_name}-{listener_name}`.

### `{consumer_name}`

The `{consumer_name}` is set to the [process name] of the Lightbus process.

### `{stream_name}`

The stream name, as described above in [sending events](#sending-events).

## Reclaiming timed-out events

Events can be considered timed if another Lightbus process has held onto them for too long.
Any client consuming events should check for these from time to time.

Timed out events can be claimed as follows:

```
# Get pending messages
XPENDING {stream} {group_name} - + {batch_size}

# For each message try to claim it
XCLAIM {stream} {group_name} {timeout}

# If successful, process the event. Otherwise ignore it
```

### `{batch_size}`

How many pending messages to fetch in each batch

### `{timeout}`

Timeout in milliseconds. This is the maximum time a Lightbus process will have to process an event 
before another processes assumes it has failed and takes over.

### Encoding & Customisation

See also: [data marshalling]

By default field values are serialised using JSON. This can be 

This encoding is customisable within the Lightbus configuration. You are welcome to use 
something custom here, but be aware that:

* A single API must have a single encoding for all events on that API
* All clients accessing the bus must be configured to use the same custom encoding

## Data validation

Validation of outgoing and incoming events is optional. However, 
validation of outgoing events is recommended as sending of event messages which fail validation may 
result in the message being rejected by any consumer.

This validation can be 
performed using the using the schema available through the [schema protocol].

## Data deformation & casting

The Lightbus client provides Python-specific functionality to streamline the 
process of moving between Python data structures and interoperable JSON data 
structures.

The level of functionality required in this regard is not specified here, and 
is left up to individual library developers.


[service name and listener name]: /explanation/events.md#service-names-listener-names
[process name]: /explanation/events.md#process-names
[data marshalling]: /explanation/marshalling.md
[schema protocol]: /reference/schema-protocol.md
