""" Serializers suitable for transports which require a single serialised value

These serializers handle moving data to/from a string-based format.

"""
from typing import Union

import lightbus
from lightbus.serializers.base import (
    decode_bytes,
    sanity_check_metadata,
    MessageSerializer,
    MessageDeserializer,
)


class BlobMessageSerializer(MessageSerializer):

    def __call__(self, message: "lightbus.Message") -> str:
        # self.encoder will typically be a json encoder, or something similar.
        # Therefore here we just return a json encoded stricture including metadata & kwargs.
        return self.encoder({"metadata": message.get_metadata(), "kwargs": message.get_kwargs()})


class BlobMessageDeserializer(MessageDeserializer):

    def __call__(self, serialized: Union[str, dict], *, native_id=None, **extra):
        # Reverse of BlobMessageSerializer

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
