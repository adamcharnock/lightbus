import inspect

import pytest

from lightbus import import_bus_module, BusPath
from lightbus.creation import get_bus
from lightbus.exceptions import FailedToImportBusModule

pytestmark = pytest.mark.unit


# import_bus_module()


def test_import_bus_module_ok(make_test_bus_module):
    bus_module_name = make_test_bus_module()
    bus_module = import_bus_module(bus_module_name)

    assert inspect.ismodule(bus_module)
    assert isinstance(bus_module.bus, BusPath)


def test_import_bus_module_does_not_exist():
    bus_module_name = "does_not_exist"
    with pytest.raises(FailedToImportBusModule) as e:
        import_bus_module(bus_module_name)

    assert "failed to import" in str(e.value).lower()


def test_import_bus_module_does_not_contain_bus(make_test_bus_module):
    bus_module_name = make_test_bus_module(code="")
    with pytest.raises(FailedToImportBusModule) as e:
        import_bus_module(bus_module_name)

    assert "attribute" in str(e.value).lower()


def test_import_bus_module_contains_bus_but_wrong_type(make_test_bus_module):
    bus_module_name = make_test_bus_module(code="bus = 123")
    with pytest.raises(FailedToImportBusModule) as e:
        import_bus_module(bus_module_name)

    assert "invalid value" in str(e).lower()
    assert "int" in str(e.value).lower()


# get_bus() - a wrapper around import_bus_module()


def test_get_bus(make_test_bus_module):
    bus_module_name = make_test_bus_module()
    bus = get_bus(bus_module_name)
    assert isinstance(bus, BusPath)
