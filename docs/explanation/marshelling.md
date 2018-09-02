Lightbus has three stages of data marshalling:

* Encode / Decode
* Serialize / Deserialize
* Validation
* Deform / Cast

## Inbound flow

Messages arriving from the bus go through the following stages
in order to prepare the data for use:

1. **Decode:** Decode the incoming data (JSON decoding by default)
2. **Deserialise:** Convert decoded data into a `Message` object
3. **Validate:** Validate the incoming message against the JSON schema
   available on the bus.
4. **Cast:** Best effort casting of parameters/results based on
   the locally available type hinting
   ([disable with `cast_values`](/reference/configuration.md#api-config))).


## Outbound flow

This is the reverse of the inbound flow. Messages being
sent will go through the following process in order to
prepare the data for transmission on bus:

1. **Deform:** Lightbus handles [NamedTuples], [dataclasses]
   and [other classes] by converting
   them into dictionaries. Other common types such as
   datetimes, Decimals etc are converted into strings.
   Internally this is referred to as the *deform* process and is
   the inverse of the *cast* process.
2. **Validate:** Validate the outgoing message against the JSON schema
   available on the bus.
2. **Serialize:** Structures the data in a way suitable for the
  transport.
3. **Encode:** Converts the data to a form suitable for transmission.
  This typically means stringifying it, for which lightbus
  uses JSON encoding by default.

## About casting

Casting is separate from validation, although both rely on type hints.
Whereas validation uses a shared
bus-wide schema to check data validity, casting uses type hints
available in the **local codebase** to marshall event and RPC parameters
into a format useful to the service's developer.




[NamedTuples]: /reference/typing.md#namedtuple-example
[dataclasses]: /reference/typing.md#dataclass-example
[other classes]: /reference/typing.md#custom-class-example
