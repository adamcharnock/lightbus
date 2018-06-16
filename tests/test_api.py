import pytest

from lightbus import Api, Event
from lightbus.exceptions import MisconfiguredApiOptions, InvalidApiEventConfiguration

pytestmark = pytest.mark.unit


def test_api_named_default():
    # Apis can not start with the name 'default'
    with pytest.raises(MisconfiguredApiOptions):

        class BadApi(Api):

            class Meta:
                name = "default"


def test_api_named_default_dot_something():
    # Apis can not start with the name 'default'
    with pytest.raises(MisconfiguredApiOptions):

        class BadApi(Api):

            class Meta:
                name = "default.foo"


def test_pass_string_as_event_params():
    # Check we cannot accidentally pass a string to Event in the
    # case that we omit a ',' when specifying a parameters tuple
    with pytest.raises(InvalidApiEventConfiguration):
        Event(parameters=("foo"))
