import pytest

import lightbus
from lightbus.exceptions import UnknownApi, EventNotFound, InvalidEventArguments, InvalidEventListener


@pytest.mark.run_loop
async def test_fire_event_api_doesnt_exist(dummy_bus: lightbus.BusNode):
    with pytest.raises(UnknownApi):
        await dummy_bus.bus_client.fire_event('non_existent_api', 'event')


@pytest.mark.run_loop
async def test_fire_event_event_doesnt_exist(dummy_bus: lightbus.BusNode, dummy_api):
    with pytest.raises(EventNotFound):
        await dummy_bus.bus_client.fire_event('my.dummy', 'bad_event')


@pytest.mark.run_loop
async def test_fire_event_bad_event_arguments(dummy_bus: lightbus.BusNode, dummy_api):
    with pytest.raises(InvalidEventArguments):
        await dummy_bus.bus_client.fire_event('my.dummy', 'my_event', kwargs={'bad_arg': 'value'})


@pytest.mark.run_loop
async def test_listen_for_event_non_callable(dummy_bus: lightbus.BusNode):
    with pytest.raises(InvalidEventListener):
        await dummy_bus.bus_client.listen_for_event('my.dummy', 'my_event', listener=123)
