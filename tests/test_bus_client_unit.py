import pytest

import lightbus
from lightbus.config import Config
from lightbus.exceptions import UnknownApi, EventNotFound, InvalidEventArguments, InvalidEventListener, \
    TransportNotFound

pytestmark = pytest.mark.unit


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


@pytest.mark.run_loop
async def test_no_transport(loop):
    # No transports configured for any relevant api
    config = Config.load_dict({
        'apis': {
            # TODO: This needs moving out of the apis config section
            'default': {'schema_transport': {'redis': {}}},
        }
    })
    bus_client = lightbus.BusClient(config=config)
    with pytest.raises(TransportNotFound):
        await bus_client.call_rpc_remote('my_api', 'test', kwargs={}, options={})


@pytest.mark.run_loop
async def test_no_transport_type(loop):
    # Transports configured, but the wrong type of transport
    config = Config.load_dict({
        'apis': {
            # TODO: This needs moving out of the apis config section
            'default': {'schema_transport': {'redis': {}}},
            'my_api': {
                'event_transport': {'redis': {}},
            }
        }
    })
    bus_client = lightbus.BusClient(config=config)
    with pytest.raises(TransportNotFound):
        await bus_client.call_rpc_remote('my_api', 'test', kwargs={}, options={})
