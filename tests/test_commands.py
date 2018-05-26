import pytest

import lightbus.commands.run
from lightbus import commands


pytestmark = pytest.mark.unit


@pytest.mark.skip(
    "We need to be able to configure the run command to use a speicifc redis instance, and "
    "haven't figured that out yet"
)
def test_commands_run():
    """make sure the arg parser is vaguely happy"""
    args = commands.parse_args(args=["run"])
    lightbus.commands.run.Command().handle(args, dry_run=True)
