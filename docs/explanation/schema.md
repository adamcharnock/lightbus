# Schema

Lightbus creates a schema for each of your APIs using the type hints 
specified on the API. This schema is shared on the bus for consumption 
by other Lightbus clients. This provides a number of features:

* The availability of a particular API can be detected by remote clients
* RPCs, results, and events transmitted on the bus can be validated by both the sender and receiver
* Lightbus tooling can load the schema to provide additional functionality

The schema is transmitted in the [JSON schema] format. 

See the [schema reference](../reference/schema.md) section for details on how this works in practice. 


[JSON schema]: https://json-schema.org/
