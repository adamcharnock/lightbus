# Transports

Transports provide the communications system for Lightbus. There are four types
of transport:

* **RPC transports** – sends and consumes RPC calls
* **Result transports** – sends and receives RPC results
* **Event transports** – sends and consumes events
* **Schema transports** – stores and retrieves the [bus schema](schema.md)

**Lightbus ships with a Redis-backed implementation of each of these transports.**
For configuration details see the [transport configuration reference](../reference/transport-configuration.md).

Lightbus can be [configured] to use custom transports either globally,
or on a per-API level.

[configured]: ../reference/configuration.md
