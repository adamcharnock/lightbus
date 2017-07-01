Test of AMQP/Kombu + ZeroMQ
===========================

Testing latency & throughput of AMQP (via Kombu), using ZeroMQ as the 
return path.

    $ python consumer_serial.py 20
    Time per call: 1281.98ms
    
    $ python consumer_parallel.py 20
    Time per call: 4.92ms


Notes
-----

* This is very slow
* The consumer spends pretty much all its time waiting on 
  rpc responses (unsurprisingly)
