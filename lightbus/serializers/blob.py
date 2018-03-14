""" Serializers suitable for transports which require a single serialised value

These serializers handle moving data to/from a string-based format.

"""

import lightbus
from lightbus.serializers.base import decode_bytes, sanity_check_metadata, MessageSerializer, MessageDeserializer


class BlobMessageSerializer(MessageSerializer):

    def __call__(self, message: 'lightbus.Message') -> str:
        # self.encoder will typically be a json encoder, or something similar.
        # Therefore here we just return a json encoded stricture including metadata & kwargs.
        return self.encoder({
            'metadata': message.get_metadata(),
            'kwargs': message.get_kwargs(),
        })


class BlobMessageDeserializer(MessageDeserializer):

    def __call__(self, serialized: str):
        # Reverse of BlobMessageSerializer
        serialized = decode_bytes(serialized)
        decoded = self.decoder(serialized)

        metadata = decoded.get('metadata', {})
        kwargs = decoded.get('kwargs', {})

        sanity_check_metadata(self.message_class, metadata)

        return self.message_class.from_dict(
            metadata=metadata,
            kwargs=kwargs,
        )




