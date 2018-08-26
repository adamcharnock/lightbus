# How to use Lightbus for metrics

Notes to be expanded upon:

* Optimising for throughput and performance. Assuming lost messages are acceptable.
* Disable all validation
* `on_error=ignore`
* Future development: Redis PUB/SUB transport?
* `cast_values=false` - You'll receive basic JSON values in the event listeners, but it'll be faster
