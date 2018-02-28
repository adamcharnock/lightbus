import pytest

import lightbus.commands.run
from lightbus import commands


pytestmark = pytest.mark.unit


def test_commands_run():
    """make sure the arg parser is vaguely happy"""
    args = commands.parse_args(args=['run'])
    lightbus.commands.run.Command().handle(args, dry_run=True)
