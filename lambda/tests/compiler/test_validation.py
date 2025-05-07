from textwrap import dedent

import pytest
from voluptuous import Invalid

from ...src.compiler.pkg.validation import no_shared_keys, output_spec


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


ospec1 = dedent("""\
    file1 -> s3://bucket/yada/yada/
        +tag1: value1
        +tag2: value2
""")

ospec2 = dedent("""\
    file2 -> s3://bucket/yada/yada/file_dos
""")

ospec3 = dedent("""\
    dirname/file3
        +tag3:with_colon: value3
        +tag4 with spaces: value4 with spaces
""")

ospec4 = dedent("""\
    file4*
""")

@pytest.mark.parametrize("ospec, expect", [
    (ospec1, {"name": "file1", "dest": "s3://bucket/yada/yada/", "s3_tags": {"tag1": "value1", "tag2": "value2"}}),
    (ospec2, {"name": "file2", "dest": "s3://bucket/yada/yada/file_dos", "s3_tags": {}}),
    (ospec3, {"name": "dirname/file3", "s3_tags": {"tag3:with_colon": "value3", "tag4 with spaces": "value4 with spaces"}}),
    (ospec4, {"name": "file4*", "s3_tags": {}}),
])
def test_output_spec(ospec, expect):
    result = output_spec(ospec)
    assert result == expect


@pytest.mark.parametrize("badspec", [
    "file1 => s3://bucket/yada/yada",  # bad arrow
    "file2 -> /bucket/yada/yada/",  # not an s3 uri
    "file3-> s3:/bucket/yada/yada",  # no space before arrow
    "file4 ->s3:/bucket/yada/yada",  # no space after arrow
    "file5 -> ",  # no destination
    "-> s3://bucket/yada/yada",  # no filename
])
def test_output_spec_bad_filename(badspec):
    with pytest.raises(Invalid, match="invalid filename spec"):
        output_spec(badspec)

@pytest.mark.parametrize("badspec", [
    "file1:\n-tag1: value1",  # tag line does not start with +
    "file2:\n+tag2:value2",  # no space after tag name
    "file2:\n+tag3: ",  # no value for tag
])
def test_output_spec_bad_tag(badspec):
    with pytest.raises(Invalid, match="invalid tag"):
        output_spec(badspec)