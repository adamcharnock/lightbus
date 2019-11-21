import asyncio
import logging
import threading

import jsonschema
import pytest
from aioredis.util import decode

import lightbus
import lightbus.path
from lightbus.config.structure import OnError
from lightbus.path import BusPath
from lightbus.config import Config
from lightbus.exceptions import LightbusTimeout, LightbusServerError
from lightbus.transports.redis.event import StreamUse
from lightbus.utilities.async_tools import cancel, block, run_user_provided_callable

pytestmark = pytest.mark.integration

stream_use_test_data = [StreamUse.PER_EVENT, StreamUse.PER_API]


@pytest.mark.timeout(5)
def benchmark_rpc(run_lightbus_command, make_test_bus_module):
    run_lightbus_command("run", "--bus", make_test_bus_module(), env={"LIGHTBUS_MODULE": ""})

    def benchmark_me():
        block(
            run_user_provided_callable(
                bus.my.dummy.my_proc.call_async,
                args=[],
                kwargs=dict(field="Hello!"),
                bus_client=bus.client,
            )
        )

    benchmark(benchmark_me)


# @pytest.mark.asyncio
# @pytest.mark.parametrize(
#     "stream_use", stream_use_test_data, ids=["stream_per_event", "stream_per_api"]
# )
# async def benchmark_event_simple(bus: lightbus.path.BusPath, dummy_api, stream_use):
#     """Full event integration test"""
#     bus.client.register_api(dummy_api)
#     bus.client.transport_registry.get_event_transport("default").stream_use = stream_use
#     received_messages = []
#
#     async def listener(event_message, **kwargs):
#         nonlocal received_messages
#         received_messages.append(event_message)
#
#     bus.my.dummy.my_event.listen(listener, listener_name="test")
#
#     await bus.client._setup_server()
#
#     await asyncio.sleep(0.1)
#     await bus.my.dummy.my_event.fire_async(field="Hello! 😎")
#     await asyncio.sleep(0.1)
#
#     assert len(received_messages) == 1
#     assert received_messages[0].kwargs == {"field": "Hello! 😎"}
#     assert received_messages[0].api_name == "my.dummy"
#     assert received_messages[0].event_name == "my_event"
#     assert received_messages[0].native_id
#
