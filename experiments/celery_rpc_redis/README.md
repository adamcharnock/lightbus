Test latency of Celery RPC (Redis)
==================================

Testing latency for simple Celery RPC, using Redis for the backend:

    $ python consumer_serial.py 20
    Time per call: 1281.98ms
    
    $ python consumer_parallel.py 20
    Time per call: 4.92ms


Notes
-----

* This is very slow
* The consumer spends pretty much all its time waiting on 
  rpc responses (unsurprisingly)
