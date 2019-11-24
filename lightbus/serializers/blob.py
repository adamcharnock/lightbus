""" Serializers suitable for transports which require a single serialised value

These serializers handle moving data to/from a string-based format.

"""
from typing import Union, TYPE_CHECKING

from lightbus.serializers.base import (
    decode_bytes,
    sanity_check_metadata,
    MessageSerializer,
    MessageDeserializer,
)

if TYPE_CHECKING:
    # pylint: disable=unused-import,cyclic-import
    from lightbus import Message


class BlobMessageSerializer(MessageSerializer):
    def __call__(self, message: "Message") -> str:
        """Takes a message object and returns a serialised string representation"""
        return self.encoder({"metadata": message.get_metadata(), "kwargs": message.get_kwargs()})


class BlobMessageDeserializer(MessageDeserializer):
    def __call__(self, serialized: Union[str, dict], *, native_id=None, **extra):
        """ Takes a serialised string representation and returns a Message object

        Reverse of BlobMessageSerializer
        """
        # Allow for receiving dicts on the assumption that this will be
        # json which has already been decoded.
        if isinstance(serialized, dict):
            decoded = serialized
        else:
            serialized = decode_bytes(serialized)
            decoded = self.decoder(serialized)

        metadata = decoded.get("metadata", {})
        kwargs = decoded.get("kwargs", {})

        sanity_check_metadata(self.message_class, metadata)

        return self.message_class.from_dict(
            metadata=metadata, kwargs=kwargs, native_id=native_id, **extra
        )
