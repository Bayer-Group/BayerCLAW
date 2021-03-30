import json

import pytest

from ...src.common.python.substitutions import _lookup, substitute_job_data, substitute_into_filenames


@pytest.mark.parametrize("target, expect", [
    ("string", "string_value"),
    ("number", "99"),
    ("list", [1,2,3]),
    ("dict", {"a":1,"b":2}),
])
def test_lookup(target, expect):
    data = {
        "string": "string_value",
        "number": 99,
        "list": [1, 2, 3],
        "dict": {"a": 1, "b": 2}
    }
    result = _lookup(target, data)
    assert isinstance(result, str)

    if target == "string" or target == "number":
        assert result == expect
    else:
        result2 = json.loads(json.loads(result))
        assert result2 == expect


def test_lookup_fail():
    data = {"a": 1, "b": 2}
    target = "z"
    with pytest.raises(RuntimeError, match="not found in job data"):
        _lookup(target, data)


def test_substitute_job_data():
    subject = {
        "a_string": "a ${job.value} b ${scatter.value} c ${parent.value}",
        "a_list": [
            "e ${job.value}",
            "f ${scatter.value}",
            "g ${parent.value}",
        ],
        "a_dict": {
            "eh": "h ${job.value}",
            "bee": "i ${scatter.value}",
            "sea": "j ${parent.value}",
        },
    }

    job_data = {
        "job": {
            "value": "one"
        },
        "scatter": {
            "value": 2
        },
        "parent": {
            "value": ["three"]
        },
    }

    expect = {
        "a_string": 'a one b 2 c "[\\"three\\"]"',
        "a_list": [
            "e one",
            "f 2",
            'g "[\\"three\\"]"',
        ],
        "a_dict": {
            "eh": "h one",
            "bee": "i 2",
            "sea": 'j "[\\"three\\"]"',
        },
    }

    result = substitute_job_data(subject, job_data)
    assert result == expect


def test_substitute_into_filenames():
    subject = {
        "file": "s3:/${bucket}/${path}/${name}.${ext}",
        "files": [
            "s3:/${bucket}/${path}/${name}1.${ext}",
            "s3:/${bucket}/${path}/${name}2.${ext}",
            "s3:/${bucket}/${path}/${name}3.${ext}",
        ],
        "fileses": {
            "file_a": "s3:/${bucket}/${path}/${name}_a.${ext}",
            "file_b": "s3:/${bucket}/${path}/${name}_b.${ext}",
            "file_c": "s3:/${bucket}/${path}/${name}_c.${ext}",
        }
    }
    subs = {
        "bucket": "bucket_name",
        "path": "path/to/whatever",
        "name": "file_name",
    }
    expect = {
        "file": "s3:/bucket_name/path/to/whatever/file_name.${ext}",
        "files": [
            "s3:/bucket_name/path/to/whatever/file_name1.${ext}",
            "s3:/bucket_name/path/to/whatever/file_name2.${ext}",
            "s3:/bucket_name/path/to/whatever/file_name3.${ext}",
        ],
        "fileses": {
            "file_a": "s3:/bucket_name/path/to/whatever/file_name_a.${ext}",
            "file_b": "s3:/bucket_name/path/to/whatever/file_name_b.${ext}",
            "file_c": "s3:/bucket_name/path/to/whatever/file_name_c.${ext}",
        }
    }
    result = substitute_into_filenames(subject, subs)
    assert result == expect
