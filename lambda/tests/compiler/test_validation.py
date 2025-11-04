from textwrap import dedent

import pytest
from voluptuous import Invalid

from ...src.compiler.pkg.validation import (no_shared_keys, shorthand_image_spec, shorthand_output_spec,
                                            file_list)


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


imgspec1 = "test_image"

imgspec2 = "test_image +auth: test_auth   "

imgspec3 = dedent("""\
    test_image
      +auth: test_auth
""")

@pytest.mark.parametrize("imgspec, expect", [
    (imgspec1, {"name": "test_image", "auth": ""}),
    (imgspec2, {"name": "test_image", "auth": "test_auth"}),
    (imgspec3, {"name": "test_image", "auth": "test_auth"}),
])
def test_image_spec(imgspec, expect):
    result = shorthand_image_spec(imgspec)
    assert result == expect


@pytest.mark.parametrize("badspec", [
    "",  # nothing
    "test_image +auth:"  # missing auth value
    "+auth test_auth",  # no image name
    "test_image +auth test_auth",  # no colon after auth
    "test_image auth: test_auth",  # no plus before auth
    "test_image another_image +auth: test_auth",  # multiple names
    "test_image +auth: test_auth another auth",  # multiple auths
    "name with spaces +auth: test_auth",  # name with spaces
    "test_image +auth: invalid_auth!!!",  # invalid characters in auth
])
def test_image_spec_fail(badspec):
    with pytest.raises(Invalid, match="invalid image spec"):
        result = shorthand_image_spec(badspec)
        print(result)

# note: value2 below has trailing spaces
ospec1 = dedent("""\
    file1 -> s3://bucket/yada/yada/
        +tag1: value1
        +tag2:  value2    
""")

ospec2 = dedent("""\
    ${job.file2}  ->   s3://bucket/${job.yadayada}/
""")

ospec3 = dedent("""\
    dirname/file3
        +tag3: colon:and+plus
        +tag4:  value4 with spaces
""")

ospec4 = dedent("""\
    file4*
""")

@pytest.mark.parametrize("ospec, expect", [
    (ospec1, {"name": "file1", "dest": "s3://bucket/yada/yada/", "s3_tags": {"tag1": "value1", "tag2": "value2"}}),
    (ospec2, {"name": "${job.file2}", "dest": "s3://bucket/${job.yadayada}/", "s3_tags": {}}),
    (ospec3, {"name": "dirname/file3", "s3_tags": {"tag3": "colon:and+plus", "tag4": "value4 with spaces"}}),
    (ospec4, {"name": "file4*", "s3_tags": {}}),
])
def test_shorthand_output_spec(ospec, expect):
    result = shorthand_output_spec(ospec)
    assert result == expect


@pytest.mark.parametrize("ospec, expect", [
    (ospec1, {"name": "file1", "dest": "s3://bucket/yada/yada/", "s3_tags": {"tag1": "value1", "tag2": "value2"}}),
    (ospec2, {"name": "${job.file2}", "dest": "s3://bucket/${job.yadayada}/", "s3_tags": {}}),
    (ospec3, {"name": "dirname/file3", "s3_tags": {"tag3": "colon:and+plus", "tag4": "value4 with spaces"}}),
])
def test_shorthand_output_spec_one_line(ospec, expect):
    one_liner = ospec.replace("\n", " ")
    result = shorthand_output_spec(one_liner)
    assert result == expect


@pytest.mark.parametrize("badspec", [
    "file1 => s3://bucket/yada/yada/",  # bad arrow
    "file2 -> /bucket/yada/yada/",  # not an s3 uri
    "file3-> s3:/bucket/yada/yada/",  # no space before arrow
    "file4 ->s3://bucket/yada/yada/",  # no space after arrow
    "file5 -> ",  # no destination after arrow
    "-> s3://bucket/yada/yada/",  # no filename
    "filename with spaces",
    "file6 -> s3://bucket/yada/yada.txt",  # destination is not a folder
])
def test_shorthand_output_spec_bad_filename(badspec):
    with pytest.raises(Invalid, match="invalid filename spec"):
        shorthand_output_spec(badspec)


@pytest.mark.parametrize("badspec", [
    "file1:\n-tag1: value1",  # tag line does not start with +
    "file2:\n+tag2:value2",  # no space after tag name
])
def test_shorthand_output_spec_bad_tag(badspec):
    with pytest.raises(Invalid, match="invalid filename spec"):
        shorthand_output_spec(badspec)


def test_file_list_dict():
    tester = file_list({str: str})

    spec = {
        "input1": "s3://bucket/path/to/input1.txt",
        "input2": "s3://bucket/path/to/input2.txt",
    }
    result = tester(spec)
    assert result == spec


def test_file_list_list():
    tester = file_list({str: str})

    spec = [
        {"input1": "s3://bucket/path/to/input1.txt"},
        {"input2": "s3://bucket/path/to/input2.txt"},
    ]
    result = tester(spec)
    expect = {
        "input1": "s3://bucket/path/to/input1.txt",
        "input2": "s3://bucket/path/to/input2.txt",
    }
    assert result == expect


def test_file_list_invalid():
    tester = file_list({str: str})

    spec = [
        {"input1": "s3://bucket/path/to/input1.txt"},
        "not-a-dict",
    ]
    with pytest.raises(Invalid, match="expected list of dicts"):
        result = tester(spec)


def test_file_list_empty():
    tester = file_list({str: str})

    spec = []
    result = tester(spec)
    expect = {}
    assert result == expect
