# Performance

!!! note "Caveats"

    Lightbus has yet to undergo any profiling or optimisation, therefore it is 
    reasonable to expect performance to improve with time.

The performance of Lightbus is primarily governed by the [transports](transports.md) used. Lightbus 
currently only ships with Redis support.

## Individual process performance

Simple benchmarking[^1] on a 2018 MacBook Pro indicates the following execution times (plugins disabled, schema 
and validation enabled, no event/RPC parameters):

* Firing an event: ≈ 1.7ms (±10%)
* Performing a remote procedure call: ≈ 6.9ms (±10%)

## Redis performance

The Redis server has the potential to be a central bottleneck in Lightbus' performance. You may start to run into 
these limits if you are sending tens thousands of events per second.

In these cases you can either:

* Scale via Redis by setting up [Redis Cluster](https://redis.io/topics/cluster-tutorial)
* Scale via Lightbus by specifying a different Redis instance per-API. See 
  [configuration](../reference/configuration.md)

[^1]: See [how to modify Lightbus](../howto/modify-lightbus.md) for details on how to run the benchmarks via `pytest`
