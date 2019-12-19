# How to use Lightbus for metrics

When we talk about metrics we may mean all or any of the following:

* Current information is most important
* Previous events will become irrelevant as soon as new data is received
* Lost events are therefore tolerable, as long as we keep up with new events
* Events may be high volume, so optimisations may be needed

!!! note

    Your needs may not precisely match this scenario, so be prepared to tweak the following configuration to your needs.

For the above metrics-based scenario, a sample Lightbus [configuration](../reference/configuration.md) may look like 
something like this:

```yaml
# Lightbus config for metrics

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
    
    # Disable validation to enhance performance
    validate:
      outgoing: false
      incoming: false
    
    # Assume we will be transmitting simple types, so we can bypass casting for performance
    cast_values: false

    event_transport:
      redis:
        url: 'redis://redis_host:6379/0'
      
        # Load in many events at once for performance improvements
        batch_size: 100
    
        # No need to keep many historical events around
        max_stream_length: 10000
      
        # Per-event streams, to allow selective consumption of metrics
        stream_use: "per_event"
```
