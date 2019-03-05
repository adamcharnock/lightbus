import pytest

from lightbus.utilities.frozendict import frozendict


@pytest.fixture()
def d():
    return frozendict(a=1, b=2)


def test_frozendict_get(d):
    assert d["a"] == 1


def test_frozendict_get_error(d):
    with pytest.raises(KeyError):
        d["z"]


def test_frozendict_contains(d):
    assert "a" in d
    assert "z" not in d


def test_frozendict_iter(d):
    i = iter(d)
    assert set(i) == {"a", "b"}


def test_frozendict_repr(d):
    assert repr(d).startswith("<frozendict {")


def test_frozendict_len(d):
    assert len(d) == 2


def test_frozendict_get(d):
    assert hash(d) == hash(d.copy())
    assert hash(d) != hash(frozendict({}))
