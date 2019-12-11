import asyncio
import logging
import time
from typing import List

from lightbus.transports.base import RpcTransport
from lightbus.api import Api
from lightbus.client.subclients.base import BaseSubClient
from lightbus.client.utilities import validate_event_or_rpc_name, queue_exception_checker
from lightbus.client.validator import validate_outgoing, validate_incoming
from lightbus.exceptions import (
    NoApisToListenOn,
    TransportIsClosed,
    SuddenDeathException,
    LightbusServerError,
    LightbusTimeout,
)
from lightbus.log import L, Bold
from lightbus.message import ResultMessage, RpcMessage
from lightbus.utilities.async_tools import run_user_provided_callable
from lightbus.utilities.casting import cast_to_signature
from lightbus.utilities.deforming import deform_to_bus
from lightbus.utilities.frozendict import frozendict
from lightbus.utilities.human import human_time

logger = logging.getLogger(__name__)


class RpcResultClient(BaseSubClient):
    """Functionality for both RPCs and results"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def consume_rpcs(self, apis: List[Api] = None):
        """Start a background task to consume RPCs

        This will consumer RPCs on APIs which have been registered with this
        bus client.
        """
        if apis is None:
            apis = self.api_registry.all()

        if not apis:
            raise NoApisToListenOn(
                "No APIs to consume on in consume_rpcs(). Either this method was called with apis=[], "
                "or the API registry is empty."
            )

        # Not all APIs will necessarily be served by the same transport, so group them
        # accordingly
        api_names = [api.meta.name for api in apis]
        api_names_by_transport = self.transport_registry.get_rpc_transport_pools(api_names)

        coroutines = []
        for rpc_transport, transport_api_names in api_names_by_transport:
            transport_apis = list(map(self.api_registry.get, transport_api_names))
            coroutines.append(
                self._consume_rpcs_with_transport(rpc_transport=rpc_transport, apis=transport_apis)
            )

        task = asyncio.ensure_future(asyncio.gather(*coroutines))
        task.add_done_callback(queue_exception_checker(self.error_queue))
        self._consumers.append(task)

    async def _consume_rpcs_with_transport(
        self, rpc_transport: RpcTransport, apis: List[Api] = None
    ):
        # TODO: InternalProducer command
        while True:
            try:
                rpc_messages = await rpc_transport.consume_rpcs(apis, bus_client=self)
            except TransportIsClosed:
                return

            for rpc_message in rpc_messages:
                validate_incoming(self.config, self.schema, rpc_message)

                await self._execute_hook("before_rpc_execution", rpc_message=rpc_message)
                try:
                    result = await self._call_rpc_local(
                        api_name=rpc_message.api_name,
                        name=rpc_message.procedure_name,
                        kwargs=rpc_message.kwargs,
                    )
                except SuddenDeathException:
                    # Used to simulate message failure for testing
                    return
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    result = e
                else:
                    result = deform_to_bus(result)

                result_message = ResultMessage(
                    result=result, rpc_message_id=rpc_message.id, api_name=rpc_message.api_name
                )
                await self._execute_hook(
                    "after_rpc_execution", rpc_message=rpc_message, result_message=result_message
                )

                if not result_message.error:
                    validate_outgoing(self.config, self.schema, result_message)

                await self.send_result(rpc_message=rpc_message, result_message=result_message)

    async def call_rpc_remote(
        self, api_name: str, name: str, kwargs: dict = frozendict(), options: dict = frozendict()
    ):
        """ Perform an RPC call

        Call an RPC and return the result.
        """
        # TODO: InternalProducer command
        rpc_transport = self.transport_registry.get_rpc_transport_pool(api_name)
        result_transport = self.transport_registry.get_result_transport_pool(api_name)

        kwargs = deform_to_bus(kwargs)
        rpc_message = RpcMessage(api_name=api_name, procedure_name=name, kwargs=kwargs)
        return_path = result_transport.get_return_path(rpc_message)
        rpc_message.return_path = return_path
        options = options or {}
        timeout = options.get("timeout", self.config.api(api_name).rpc_timeout)
        # TODO: rpc_timeout is in three different places in the config!
        #       Fix this. Really it makes most sense for the use if it goes on the
        #       ApiConfig rather than having to repeat it on both the result & RPC
        #       transports.
        validate_event_or_rpc_name(api_name, "rpc", name)

        logger.info("📞  Calling remote RPC {}.{}".format(Bold(api_name), Bold(name)))

        start_time = time.time()
        # TODO: It is possible that the RPC will be called before we start waiting for the
        #       response. This is bad.

        validate_outgoing(self.config, self.schema, rpc_message)

        future = asyncio.gather(
            self.receive_result(rpc_message, return_path, options=options),
            rpc_transport.call_rpc(rpc_message, options=options, bus_client=self),
        )

        await self._execute_hook("before_rpc_call", rpc_message=rpc_message)

        try:
            result_message, _ = await asyncio.wait_for(future, timeout=timeout)
            future.result()
        except asyncio.TimeoutError:
            # Allow the future to finish, as per https://bugs.python.org/issue29432
            try:
                await future
                future.result()
            except asyncio.CancelledError:
                pass

            # TODO: Remove RPC from queue. Perhaps add a RpcBackend.cancel() method. Optional,
            #       as not all backends will support it. No point processing calls which have timed out.
            raise LightbusTimeout(
                f"Timeout when calling RPC {rpc_message.canonical_name} after {timeout} seconds. "
                f"It is possible no Lightbus process is serving this API, or perhaps it is taking "
                f"too long to process the request. In which case consider raising the 'rpc_timeout' "
                f"config option."
            ) from None

        await self._execute_hook(
            "after_rpc_call", rpc_message=rpc_message, result_message=result_message
        )

        if not result_message.error:
            logger.info(
                L(
                    "🏁  Remote call of {} completed in {}",
                    Bold(rpc_message.canonical_name),
                    human_time(time.time() - start_time),
                )
            )
        else:
            logger.warning(
                L(
                    "⚡ Server error during remote call of {}. Took {}: {}",
                    Bold(rpc_message.canonical_name),
                    human_time(time.time() - start_time),
                    result_message.result,
                )
            )
            raise LightbusServerError(
                "Error while calling {}: {}\nRemote stack trace:\n{}".format(
                    rpc_message.canonical_name, result_message.result, result_message.trace
                )
            )

        validate_incoming(self.config, self.schema, result_message)

        return result_message.result

    async def _call_rpc_local(self, api_name: str, name: str, kwargs: dict = frozendict()):
        # TODO: InternalProducer command
        api = self.api_registry.get(api_name)
        validate_event_or_rpc_name(api_name, "rpc", name)

        start_time = time.time()
        try:
            method = getattr(api, name)
            if self.config.api(api_name).cast_values:
                kwargs = cast_to_signature(kwargs, method)
            result = await run_user_provided_callable(
                method, args=[], kwargs=kwargs, error_queue=self.error_queue
            )
        except (asyncio.CancelledError, SuddenDeathException):
            raise
        except Exception as e:
            logging.exception(e)
            logger.warning(
                L(
                    "⚡  Error while executing {}.{}. Took {}",
                    Bold(api_name),
                    Bold(name),
                    human_time(time.time() - start_time),
                )
            )
            raise
        else:
            logger.info(
                L(
                    "⚡  Executed {}.{} in {}",
                    Bold(api_name),
                    Bold(name),
                    human_time(time.time() - start_time),
                )
            )
            return result
