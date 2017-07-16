## Archived suggestions

### Consider MQTT rather than AMQP

Questions to be answered:

* Does MQTT support loosely coupling processes as described above?
* What brokers are available?
* How do MQTT brokers compare to RabbitMQ et al? Stability, features, etc
* How easily can the brokers be deployed?

**Quotes of note:**

> We recommend the use of AMQP protocol to build reliable,scalable, and advanced clustering messaging infrastructuresover an ideal WLAN, and the use of MQTT protocol to supportconnections with edge nodes (simple sensors/actuators) underconstrained environments (low-speed wireless access) 

[source](http://sci-hub.io/10.1109/ccnc.2015.7158101)

### Consider Kafka rather than AMQP

Questions to be answered:

* Does Kafka support loosely coupling processes as described above?
* How easily can Kafka be deployed?
* **Update:** Kafka appears more difficult to setup. Plus we'd tie ourselves to 
  an implementation, rather than a protocol (in the case of AMQP)
