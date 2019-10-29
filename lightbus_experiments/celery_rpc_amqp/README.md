Test latency of Celery RPC (AMQP)
=================================

Testing latency for simple Celery RPC, using AMQP for the backend:

    $ python consumer_serial.py 20
    Time per call: 1296.86ms
    
    $ python consumer_parallel.py 20
    Time per call: 8.1ms


Notes
-----

* This is very slow
* The consumer spends pretty much all its time waiting on 
  rpc responses (unsurprisingly)
