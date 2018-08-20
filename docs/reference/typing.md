
## Quick notes

Will be improved upon later.

Inbound: `decode -> deserialise -> cast`

Outbound: `deform -> serialize -> encode`

* Types not transmitted with data
* Validation:
    * Data validated against the schema
    * API schemas are shared on the bus
* Outbound:
    * Data is encoded as JSON by default
    * Therefore all values are transmitted as JSON-safe types
    * **Deform:** Lightbus handles NamedTuples, dataclasses by converting
      them into dictionaries. Other common types such as
      datetimes, Decimals etc are converted into strings.
      Internally this is referred to as the *deform* process.
    * **Serialize:** Structures the data in a way suitable for the
      transport.
    * **Encode:** Converts the data to a form suitable for transmission.
      This typically means stringifying it, for which lightbus
      uses JSON encoding by default.
* Inbound (the reverse of the outbound process)
    * **Decode:** JSON decode (by default) the incoming data
    * **Deserialise:** Convert the data from its bus-level structure
    * **Cast:** Best effort casting of parameters/results based on the
      type hinting provided in the calling code.


Cusom objects can be transmitted on the bus as long as they define the
`__to_bus__()` magic method. Conversely, custom objects can be recieved
by providing a type hint for the class, where the class defines the
`__from_bus__(value)` static method. For example:


```python3
class UserDatabaseObject(object):
    def __to_bus__(self):
        return self.id

    @classmethod
    def __from_bus__(cls, value):
        return cls.objects.get(id=value)
```
