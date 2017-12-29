import pytest

from lightbus import commands


pytestmark = pytest.mark.unit


@pytest.mark.run_loop
async def test_commands_run():
    """make sure the arg parser is vaguely happy"""
    args = commands.parse_args(args=['run'])
    commands.command_run(args, dry_run=True)
