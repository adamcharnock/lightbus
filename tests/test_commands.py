import pytest

import lightbus
from lightbus import commands
from lightbus.bus import BusNode
from lightbus.exceptions import InvalidBusNodeConfiguration


@pytest.mark.run_loop
async def test_commands_run():
    """make sure the arg parser is vaguely happy"""
    args = commands.parse_args(args=['run'])
    commands.command_run(args, dry_run=True)
