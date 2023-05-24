import pytest
from voluptuous import Invalid

from ...src.compiler.pkg.validation import no_shared_keys


@pytest.fixture(scope="module")
def no_shared_keys_func():
    ret = no_shared_keys("inputs", "outputs", "other_stuff")
    return ret


def test_no_shared_keys_pass(no_shared_keys_func):
    record = {
        "inputs": {"a": 1, "b": 2},
        "outputs": {"c": 1, "d": 2},
        "other_stuff": {"e": "3"},
    }
    result = no_shared_keys_func(record)
    assert result == record


def test_no_shared_keys_fail(no_shared_keys_func):
    record = {
        "inputs": {"a": 1, "b": 2, "x": 9, "y": 12},
        "outputs": {"a": 1, "d": 2, "x": 10, "z": 12},
        "other_stuff": {"b": 3, "d": 4, "x": 11, "t": 12},
    }

    with pytest.raises(Invalid, match="duplicated keys: a, b, d, x"):
        no_shared_keys_func(record)
