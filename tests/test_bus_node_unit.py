import pytest

import lightbus
from lightbus.bus import BusNode
from lightbus.exceptions import InvalidBusNodeConfiguration, InvalidParameters

pytestmark = pytest.mark.unit


@pytest.mark.run_loop
async def test_init_root_with_name():
    with pytest.raises(InvalidBusNodeConfiguration):
        BusNode(name="root", parent=None, client=None)


@pytest.mark.run_loop
async def test_ancestors():
    root_node = BusNode(name="", parent=None, client=None)
    child_node1 = BusNode(name="my_api", parent=root_node, client=None)
    child_node2 = BusNode(name="auth", parent=child_node1, client=None)
    assert list(child_node2.ancestors(include_self=True)) == [child_node2, child_node1, root_node]


@pytest.mark.run_loop
async def test_fully_qualified_name():
    root_node = BusNode(name="", parent=None, client=None)
    child_node1 = BusNode(name="my_api", parent=root_node, client=None)
    child_node2 = BusNode(name="auth", parent=child_node1, client=None)
    assert root_node.fully_qualified_name == ""
    assert child_node1.fully_qualified_name == "my_api"
    assert child_node1.fully_qualified_name == "my_api"
    assert str(child_node2) == "my_api.auth"


@pytest.mark.run_loop
async def test_dir(dummy_bus: lightbus.BusNode, dummy_api):
    assert "my" in dir(dummy_bus)
    assert "dummy" in dir(dummy_bus.my)
    assert "my_event" in dir(dummy_bus.my.dummy)
    assert "my_proc" in dir(dummy_bus.my.dummy)

    # Make sure we don't error if the api/rpc/event doesn't exist
    dir(dummy_bus.foo)
    dir(dummy_bus.foo.bar)


@pytest.mark.run_loop
async def test_positional_only_rpc(dummy_bus: lightbus.BusNode):
    with pytest.raises(InvalidParameters):
        await dummy_bus.my.dummy.my_proc.call_async(123)


@pytest.mark.run_loop
async def test_positional_only_event(dummy_bus: lightbus.BusNode):
    with pytest.raises(InvalidParameters):
        await dummy_bus.my.dummy.event.fire_async(123)
