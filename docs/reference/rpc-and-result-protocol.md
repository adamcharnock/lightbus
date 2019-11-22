# RPC & result protocol (Redis)

Here we document the specific interactions between Lightbus and Redis. 
The concrete implementation of this is provided by the `RedisRpcTransport` and 
`RedisResultTransport` classes. Before reading you should be familiar with Lightbus' [data marshalling].

This documentation may be useful when debugging, developing third-party client libraries, 
or simply for general interest and review. **You do not need to be aware of this 
protocol in order to use Lightbus**.

## Sending RPC Calls (client)

The following Redis commands will send a remote procedure call:

    RPUSH "{api_name}:rpc_queue" "{blob_serialized_message}"
    SET "rpc_expiry_key:{rpc_call_message_id}"
    EXPIRE "rpc_expiry_key:{rpc_call_message_id}" {rpc_timeout_seconds:5}

See [message serialisation & encoding](#message-serialisation-encoding) for the format of 
`{blob_serialized_message}`.

### The return path

Each RPC message must specify a `return_path` in its metadata. This states how the client 
expects to receive the result of the RPC. Both the client and the server must be able to 
comprehend and act up the provided return path, otherwise results will fail to be communicated.

Assuming you are using the built-in Redis RPC result transport, the return path 
should be in the following format:

    redis+key://{api_name}.{rpc_name}:result:{rpc_call_message_id} 

The server will place the RPC result into a Redis key. The value following 
`redis+key://` will be used as the key name.

## Receiving RPC Results (client)

The following Redis commands will block and await the result of an RPC call:

    BLPOP "{api_name}.{rpc_name}:result:{rpc_call_message_id}"

This simply waits from a value to appear within the key specified by the `return_path`
in the sent RPC message.

See [message serialisation & encoding](#message-serialisation-encoding) for the format of 
the returned RPC result.

## Consuming incoming RPC calls (server)

RPCs are consumed as follows:

    # Blocks until a RPC message is received
    BLPOP "{api_name}:rpc_queue"
    
    DEL "rpc_expiry_key:{rpc_call_message_id}"
    # If DEL returns 1 (key deleted) then execute the RPC
    # If DEL returns 0 (key did not exist) then ignore the RPC
    
    # Parse blob-serialised RPC message, Execute RPC, get result
    
    LPUSH {redis_key_specific_in_return_path} {blob_serialized_result}
    EXPIRE {redis_key_specific_in_return_path} {result_ttl_seconds}

## Message serialisation & encoding

Above we have often referred to `{blob_serialized_message}` 
or `{blob_serialized_result}`. These are JSON blobs of data 
in a specific format.

### Serialisation

See also: [data marshalling]

Within the Redis RPC transports all messages are serialised into a single 
value. This value is referred to as a 'blob'. This serialisation is 
performed by the `BlobMessageSerializer` and `BlobMessageDeserializer` classes.

#### RPC Message

This is an outgoing RPC message body. This RPC message represents a request that an RPC be executed and a response be
returned.

```python3
{
    "metadata": {
        # Unique base64-encoded UUID
        "id": "KrXz5EUXEem2gazeSAARIg==",
        # Fully qualified API name
        "api_name": "my_company.auth",
        # The name of the remote procedure call
        "procedure_name": "check_password",
        # How and where should the result be sent
        "return_path": "redis+key://my_company.auth.check_password:result:KrXz5EUXEem2gazeSAARIg==",
    },
    
    # Key/value arguments potentiually needed to execute the remote procedure call
    "kwargs": {
        "username": "adam",
        "password": "secret",
        ...
    }
}
```

#### Result Message

This is the response message indicating an RPC has been 
(successfully or unsuccessfully) executed.

```python3
{
    "metadata": {
        # The newly randomly generated ID of this RPC result message.
        # (base64-encoded UUID)
        "id": "L4iXaEUgEemnBazeSAARIg==",
        # The ID of the received RPC call message
        "rpc_message_id": "KrXz5EUXEem2gazeSAARIg==",
        # String representation of any error which occurred
        "error": "",
        # Error stack trace. Only used in event of error, may be omitted otherwise
        "trace": "{error_stack_trace}",
    },
    
    # The result as returned by the executed RPC
    "result": ...
}
```

### Encoding & Customisation

See also: [data marshalling]

The message must be encoded as a string once it has been serialised to 
the above blob structure. The default implementation is to use JSON 
as this encoding.

This encoding is customisable within the Lightbus configuration. You are welcome to use 
something custom here, but be aware that:

* A single API must have a single encoding for all RPCs on that API
* All clients accessing the bus must be configured to use the same custom encoding

## Data validation

Validation of outgoing parameters and incoming results is optional. However, 
validation of outgoing parameters is recommended as sending of RPC messages which fail validation may 
result in the message being rejected by the Lightbus server.  

This validation can be 
performed using the using the schema available through the [schema protocol].

## Data deformation & casting

The Lightbus client provides Python-specific functionality to streamline the 
process of moving between Python data structures and interoperable JSON data 
structures.

The level of functionality required in this regard is not specified here, and 
is left up to individual library developers.

[data marshalling]: ../explanation/marshalling.md
[schema protocol]: schema-protocol.md
