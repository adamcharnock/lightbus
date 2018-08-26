# How to use Lightbus for event sourcing

Notes to be expanded upon:

* We are optimising for reliability and completeness
* Specify consumer group to event listeners
* Validate outgoing messages, do not validate incoming messages
* Set `max_stream_length` high (or to `null`)
* Transactional transport where an RDBMS is written to in event handlers (experimental)
* `on_error=shutdown`
