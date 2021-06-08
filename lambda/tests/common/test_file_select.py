import json as j
import os

import boto3
import moto
import pytest


from ...src.common.python.file_select import select_file_contents, read_json, read_yaml
csv = b"""\
id,one,two,three,four
a,11,12,13,14
b,21,22,23,24
c,31,32,33,34
d,41,42,43,44
"""

json = b"""\
[
    {"a":11, "b":12, "c":13, "d":14},
    {"a":21, "b":22, "c":23, "d":24},
    {"a":31, "b":32, "c":33, "d":34},
    {"a":41, "b":42, "c":43, "d":44}
]
"""

jsonl = b"""\
{"a":11, "b":12, "c":13, "d":14}
{"a":21, "b":22, "c":23, "d":24}
{"a":31, "b":32, "c":33, "d":34}
{"a":41, "b":42, "c":43, "d":44}
"""

tsv = b"""\
id	one	two	three	four
a	11	12	13	14
b	21	22	23	24
c	31	32	33	34
d	41	42	43	44
"""

txt = b"""\
row1
row2
row3
row4
row5
"""

yaml = b"""\
data:
  - a: 11
    b: 12
    c: 13
    d: 14
  - a: 21
    b: 22
    c: 23
    d: 24
  - a: 31
    b: 32
    c: 33
    d: 34
  - a: 41
    b: 42
    c: 43
    d: 44
"""


@pytest.fixture(scope="module")
def src_bucket():
    with moto.mock_s3():
        yld = boto3.resource("s3", region_name="us-east-1").Bucket("test-bucket")
        yld.create()
        yld.put_object(Key="test-data/file.csv", Body=csv)
        yld.put_object(Key="test-data/file.json", Body=json)
        yld.put_object(Key="test-data/file.jsonl", Body=jsonl)
        yld.put_object(Key="test-data/file.tsv", Body=tsv)
        yld.put_object(Key="test-data/file.txt", Body=txt)
        yld.put_object(Key="test-data/file.yaml", Body=yaml)
        yield yld


@pytest.mark.parametrize("query, expect", [
    ("s3://test-bucket/test-data/file.json:$[*].d", ["14", "24", "34", "44"]),          # select all "d" elements
    ("s3://test-bucket/test-data/file.jsonl:$[*].c", ["13", "23", "33", "43"]),         # select all "c" elements
    ("s3://test-bucket/test-data/file.yaml:$.data[*].a", ["11", "21", "31", "41"]),     # select all "a" elements
    ("s3://test-bucket/test-data/file.csv:$[*].three", ["13", "23", "33", "43"]),       # select column "three"
    ("s3://test-bucket/test-data/file.csv:$[*].two", ["12", "22", "32", "42"]),         # select column "two"
    ("s3://test-bucket/test-data/file.txt:$[2:4]", ["row3", "row4"]),                   # select lines 2 and 3 (zero-based)
    ("s3://test-bucket/test-data/file.txt", ["row1", "row2", "row3", "row4", "row5"]),  # select all lines
])
def test_select_file_contents(src_bucket, query, expect):
    result = select_file_contents(query)
    print(str(result))
    assert result == expect


"""
Tests: file_select.read_json(body)
This converts a file with a JSON-like structure into JSON format
"""
# @pytest.mark.skip
def test_read_json0(tmp_path):
    json_data = {"key": "value"}
    json_file = tmp_path / "test.json"
    with json_file.open(mode="w") as fp:
        j.dump(json_data, fp)

    # read in file contents in memory
    with json_file.open(mode="r") as json_content:
        response = read_json(
            body=json_content
        )

        assert(response == json_data)


"""
Tests: file_select.read_yaml(body)
Test the Conversion of YAML into a dictionary object
"""
#@pytest.mark.skip()
def test_read_yaml0():

    # expected response
    response_should_be = {'a': 1, 'b': {'c': 3, 'd': 4}}

    # test input value to function
    input_body = """
    a: 1
    b:
        c: 3
        d: 4
    """

    response = read_yaml (
        body=input_body
    )

    assert(response == response_should_be)
