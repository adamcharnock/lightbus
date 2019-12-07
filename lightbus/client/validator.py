import logging
from typing import Union

from lightbus.exceptions import UnknownApi
from lightbus.message import EventMessage, RpcMessage, ResultMessage
from lightbus.schema import Schema
from lightbus.config import Config

logger = logging.getLogger(__name__)


def validate_incoming(
    config: Config, schema: Schema, message: Union[EventMessage, RpcMessage, ResultMessage]
):
    return _validate(config, schema, message, "incoming")


def validate_outgoing(
    config: Config, schema: Schema, message: Union[EventMessage, RpcMessage, ResultMessage]
):
    return _validate(config, schema, message, "outgoing")


def _validate(
    config, schema, message: Union[EventMessage, RpcMessage, ResultMessage], direction: str
):
    if direction not in ("incoming", "outgoing"):
        raise AssertionError("Invalid direction specified")

    # Result messages do not carry the api or procedure name, so allow them to be
    # specified manually
    api_name = message.api_name
    event_or_rpc_name = getattr(message, "procedure_name", None) or getattr(message, "event_name")
    api_config = config.api(api_name)
    strict_validation = api_config.strict_validation

    if not getattr(api_config.validate, direction):
        return

    if api_name not in schema:
        if strict_validation:
            raise UnknownApi(
                f"Validation is enabled for API named '{api_name}', but there is no schema present for this API. "
                f"Validation is therefore not possible. You are also seeing this error because the "
                f"'strict_validation' setting is enabled. Disabling this setting will turn this exception "
                f"into a warning. "
            )
        else:
            logger.warning(
                f"Validation is enabled for API named '{api_name}', but there is no schema present for this API. "
                f"Validation is therefore not possible. You can force this to be an error by enabling "
                f"the 'strict_validation' config option. You can silence this message by disabling validation "
                f"for this API using the 'validate' option."
            )
            return

    if isinstance(message, (RpcMessage, EventMessage)):
        schema.validate_parameters(api_name, event_or_rpc_name, message.kwargs)
    elif isinstance(message, ResultMessage):
        schema.validate_response(api_name, event_or_rpc_name, message.result)
