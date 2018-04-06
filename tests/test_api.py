import pytest

from lightbus import Api
from lightbus.exceptions import MisconfiguredApiOptions

pytestmark = pytest.mark.unit


def test_api_named_default():
    with pytest.raises(MisconfiguredApiOptions):
        class BadApi(Api):
            class Meta:
                name = 'default'


def test_api_named_default_dot_something():
    with pytest.raises(MisconfiguredApiOptions):
        class BadApi(Api):
            class Meta:
                name = 'default.foo'
