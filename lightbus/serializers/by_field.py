""" Serializers suitable for transports which support multiple fields per message

These serializers handle moving data to/from a dictionary
format. The format looks like this::

    # Message metadata first. Each value is implicitly a utf8 string
    id: 'ZOCTLh1CEeimW3gxwcOTbg=='
    api_name: 'my_company.auth'
    procedure_name: 'check_password'
    return_path: 'redis+key://my_company.auth.check_password:result:ZOCTLh1CEeimW3gxwcOTbg=='

    # kwargs follow, each encoded with the provided encoder (in this case JSON)
    kw:username: '"admin"'
    kw:password: '"secret"'

"""
from typing import TYPE_CHECKING

from lightbus.serializers.base import (
    decode_bytes,
    sanity_check_metadata,
    MessageSerializer,
    MessageDeserializer,
)

if TYPE_CHECKING:
    # pylint: disable=unused-import,cyclic-import
    from lightbus import Message


class ByFieldMessageSerializer(MessageSerializer):
    def __call__(self, message: "Message") -> dict:
        """Takes a message object and returns a serialised dictionary representation

        See the module-level docs (above) for further details
        """
        serialized = message.get_metadata()
        for k, v in message.get_kwargs().items():
            serialized[":{}".format(k)] = self.encoder(v)
        return serialized


class ByFieldMessageDeserializer(MessageDeserializer):
    def __call__(self, serialized: dict, *, native_id=None, **extra):
        """Takes a dictionary of serialised fields and returns a Message object

        See the module-level docs (above) for further details
        """
        metadata = {}
        kwargs = {}

        for k, v in serialized.items():
            k = decode_bytes(k)
            v = decode_bytes(v)

            if not k:
                continue

            # kwarg fields start with a ':', everything else is metadata
            if k[0] == ":":
                # kwarg values need decoding
                kwargs[k[1:]] = self.decoder(v)
            else:
                # metadata args are implicitly strings, so we don't need to decode them
                metadata[k] = v

        sanity_check_metadata(self.message_class, metadata)

        if "native_id" in metadata:
            native_id = metadata.pop("native_id")

        return self.message_class.from_dict(
            metadata=metadata, kwargs=kwargs, native_id=native_id, **extra
        )
