Test of AMQP/Kombu + ZeroMQ
===========================

Testing latency & throughput of AMQP (via Kombu), using ZeroMQ as the 
return path.

    $ python producer.py 10000
    Time per put: 0.8ms
    Puts per second: 1257.15
    ZeroMQ time: 6420.75ms
    AMQP time: 1324.4ms
    
    $ python consumer.py 10000
    Waiting for kick-off message from producer
    Got it! Let's go...
    Time per get: 0.8ms
    Gets per second: 1257.31
    ZeroMQ time: 156.04ms
    AMQP time: 7672.79ms


Notes
-----

* Results are returned to the producer via ZeroMQ
* Compared to the RPC celery experiment this is exceptionally fast
* Requires direct network access between producer and consumer
* I believe the high ZeroMQ time in the producer is caused by the 
  high AMQP time in the consumer. Pre-fetching results from AMQP may 
  alleviate this.
