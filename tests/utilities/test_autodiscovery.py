from pathlib import Path

import pytest

from lightbus.utilities.autodiscovery import prepare_exec_for_file, autodiscover

pytestmark = pytest.mark.unit
ROOT_DIR = Path(__file__).parent.parent.parent / "lightbus"


def test_prepare_exec_for_file_normal():
    assert prepare_exec_for_file(ROOT_DIR / "message.py") == "lightbus.message"


def test_prepare_exec_for_file_init_py():
    assert prepare_exec_for_file(ROOT_DIR / "utilities" / "__init__.py") == "lightbus.utilities"


def test_prepare_exec_for_file_non_py():
    with pytest.raises(Exception):
        assert prepare_exec_for_file(ROOT_DIR / "message.foo")


def test_autodiscover_no_path():
    search_directory = ROOT_DIR.parent / "lightbus_examples" / "ex01_quickstart_procedure"
    module = autodiscover(search_directory=search_directory)
    assert module.__file__ == str(search_directory / "bus.py")


def test_autodiscover_with_path():
    search_directory = ROOT_DIR.parent / "lightbus_examples" / "ex01_quickstart_procedure"
    module = autodiscover(search_directory=search_directory, bus_path=search_directory / "bus.py")
    assert module.__file__ == str(search_directory / "bus.py")


def test_autodiscover_no_result():
    search_directory = ROOT_DIR.parent / "tests"
    module = autodiscover(search_directory=search_directory)
    assert module is None
