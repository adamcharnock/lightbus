# Schema

Lightbus creates a schema for each of your APIs using the type hints 
specified on the API. This schema is shared on the bus for consumption 
by other Lightbus clients. This provides a number of features:

* The availability of a particular API can be detected by remote clients
* RPCs, results, and events transmitted on the bus can be validated by both the sender and receiver
* Lightbus tooling can load the schema to provide additional functionality

Note that an API's schema will only be available on the bus while there is a worker 
running to provides it. Once the worker process for an API shuts down the schema on the 
bus will be cleaned up shortly thereafter.

## See also

See the [schema reference](../reference/schema.md) section for details on how this works in practice.
 
The schema is created using the [JSON schema] format, see the [schema protocol] for details of 
the transmission format.


[JSON schema]: https://json-schema.org/
[schema protocol]
