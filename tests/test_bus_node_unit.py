import pytest

import lightbus
from lightbus.bus import BusNode
from lightbus.exceptions import InvalidBusNodeConfiguration


@pytest.mark.run_loop
async def test_init_root_with_name():
    with pytest.raises(InvalidBusNodeConfiguration):
        BusNode(name='root', parent=None, bus_client=None)
