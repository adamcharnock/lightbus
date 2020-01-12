# How to use Lightbus for event sourcing

We won't go into the details of event sourcing here, but we can roughly 
describe our messaging needs as follows:

* We are optimising for reliability and completeness, performance is secondary
* Sent events must be valid
* Received events must be processed regardless of their validity
* Event history is very important

!!! note

    Your needs may not precisely match this scenario, so be prepared to tweak the following configuration to your needs.

## Global configuration

For the above event sourced scenario, a sample Lightbus [configuration](../reference/configuration.md) may look like 
something like this:

```yaml
# Lightbus config for event sourcing

bus:
  schema:
    transport:
      redis:
        url: "redis://redis_host:6379/0"

apis:
  
  # Here we specify the default for your entire bus, but you could
  # also specify the config for a specific API by using the API's name
  # instead of 'default'.
  default:
    
    validate:
      # Sent (outgoing) events must be valid
      outgoing: true
      # Received (incoming) events must be processed 
      # regardless of their validity
      incoming: false

    event_transport:
      redis:
        url: 'redis://redis_host:6379/0'
      
        # Load few events into order to prioritise consistency
        batch_size: 1
    
        # Do not truncate the event stream. We keep all events 
        # as these events are our source of truth
        max_stream_length: null
      
        # Per-API streams, as we wish to prioritise 
        # ordering (this is the default)
        stream_use: "per_api"
```

## Run a single Lightbus worker

Running only a single Lightbus worker process 
(with a specific process name) will ensure messages are processed in 
order.

    lightbus run --service-name=example_service --process-name=worker

## Set process names

If your event volume requires multiple workers then ensure you set
a deterministic per-process name for each. 
This will allow restarted workers to immediately pickup any previously claimed messages 
without needing to wait for a timeout.

For example, if you have three Lightbus workers you can start each as follows:

    lightbus run --service-name=example_service --process-name=worker1
    lightbus run --service-name=example_service --process-name=worker2
    lightbus run --service-name=example_service --process-name=worker3

**Ordering will *not* be maintained when running multiple workers.**
