# Internal Architecture

Lightbus' internal workings are composed of:

* **The user-facing API**. This is provided by the `BusClient` class, which then makes use of the `EventClient` and
  `RpcResultClient` classes. This is a friendly API that issues helpful errors where appropriate. This also
  orchestrates system startup and shutdown.
* **An internal message queuing system**. This includes four separate internal message queues plus the `EventDock` 
  & `RpcResultDock` classes. The message queues provide the internal communication medium between the
  user-facing API and the Lightbus backend. The `EventDock` & `RpcResultDock` classes convert these messages 
  into a simplified API for implementation by the transports. The `EventDock` contains the `EventTransport`, 
  and the `RpcResultDock` contains both the `RpcTransport` and `RpcResultTransport`.
* **The Event & RPC transports** implement Lightbus functionality for a specific backend (e.g. Redis). The main transports 
  shipped with Lightbus are the `RedisEventTransport`, `RedisRpcTransport`, and `RedisResultTransport`.

## Diagram

![Internal Architecture Diagram][diagram]

[diagram]: ../static/images/internal-architecture.png

