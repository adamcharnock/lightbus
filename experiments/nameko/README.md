Nameko performance test
=======================

    $ nameko shell
    >>> import timeit
    >>> timeit.timeit(lambda: n.rpc.greeting_service.hello(name="ナメコ"), number=1000)
    2.4728409260278568

Notes
-----

* The competition :s
* About 2.5ms per call, achieved with RPC going via Kombu only.
* Kombu + ZeroMQ gives 0.8ms. However, presumably Nameko adds some 
  overhead (just as Warren would), so this may not be a fair 
  comparison.
