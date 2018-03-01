""" Serializers suitable for transports which support multiple fields per message

These serializers handle moving data to/from a dictionary
format. The format looks like this::

    # Message metadata first. Each value is implicitly a utf8 string
    rpc_id: 'ZOCTLh1CEeimW3gxwcOTbg=='
    api_name: 'my_company.auth'
    procedure_name: 'check_password'
    return_path: 'redis+key://my_company.auth.check_password:result:ZOCTLh1CEeimW3gxwcOTbg=='

    # kwargs follow, each encoded with the provided encoder (in this case JSON)
    kw:username: '"admin"'
    kw:password: '"secret"'

"""

import inspect
import json

from typing import Type

from lightbus import Message
from lightbus.exceptions import InvalidSerializerConfiguration, InvalidMessage


class ByFieldMessageSerializer(object):

    def __init__(self, encoder=json.dumps):
        self.encoder = encoder

    def __call__(self, message: Message) -> dict:
        """Takes a message object and returns a serialised dictionary representation

        See the module-level docs (above) for further details
        """
        serialized = message.get_metadata()
        for k, v in message.get_kwargs().items():
            serialized[':{}'.format(k)] = self.encoder(v)
        return serialized


class ByFieldMessageDeserializer(object):

    def __init__(self, message_class: Type[Message], decoder=json.loads):
        if not inspect.isclass(message_class):
            raise InvalidSerializerConfiguration(
                "The message_class value provided to JsonMessageDeserializer was not a class, "
                "it was actually: {}".format(message_class)
            )

        self.message_class = message_class
        self.decoder = decoder

    def __call__(self, serialized: dict):
        """Takes a dictionary of serialised fields and returns a Message object

        See the module-level docs (above) for further details
        """
        metadata = {}
        kwargs = {}

        for k, v in serialized.items():
            # kwarg fields start with a ':', everything else
            # is metadata
            if k[0] == ':':
                # kwarg values need decoding
                kwargs[k[1:]] = self.decoder(v)
            else:
                # metadata args are implicitly strings, so we don't need to decode them
                metadata[k] = v

        self.sanity_check_metadata(metadata)

        return self.message_class.from_dict(
            metadata=metadata,
            kwargs=kwargs,
        )

    def sanity_check_metadata(self, metadata):
        """Takes unserialized metadata and checks it looks sane

        This relies upon the required_metadata of each Message class
        to provide a list of metadata fields that are required.
        """
        for required_key in self.message_class.required_metadata:
            if required_key not in metadata:
                raise InvalidMessage(
                    "Required key {key} missing in {cls} metadata. "
                    "Found keys: {keys}".format(
                        key=required_key,
                        keys=', '.join(metadata.keys()),
                        cls=self.message_class.__name__
                    )
                )
            elif not metadata.get(required_key):
                raise InvalidMessage(
                    "Required key {key} present in {cls} metadata but value was empty"
                    "".format(
                        key=required_key,
                        cls=self.message_class.__name__
                    )
                )
