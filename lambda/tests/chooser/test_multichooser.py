import json
import logging

import boto3
from box import Box, BoxList
import moto
import pytest

from ...src.chooser.multichooser import load_s3_object, load_vals, eval_this, run_exprs,\
    lambda_handler, ConditionFailed

logging.basicConfig(level=logging.INFO)

job_data = {
    "job": {
        "eh": True,
        "bee": {
            "sea": ["dee", "ee", "eff"],
            "gee": {"aytch": "eye"},
        },
        "file_too": "file2",
    },
    "scatter": {"do not": "use"},
    "parent": {"do not": "use"},
}

data0 = { "input1": 99,
          "x": "zee",
          "y": "why",
          "z": "ecks",}
data1 = {"a": 1,
         "b": [2, 3, 4],
         "c": {"d": 5, "e": 6}}
data2 = {"f": 7,
         "g": [8, 9],
         "h": {"i": 10, "j": 11},
         "k": True,
         "l": False}
data3 = [12,
         [13, 14, 15],
         {"m": {"a": 1, "b": 2}},
         "wut"]
data4 = [99, 98, 97]
data5 = "sasquatch"


@pytest.fixture(scope="module")
def mock_repo():
    with moto.mock_aws():
        bucket_name = "repo-bucket"
        repo_name = "repo"

        s3 = boto3.resource("s3", region_name="us-east-1")
        bucket = s3.create_bucket(Bucket=bucket_name)

        bucket.put_object(
            Body=json.dumps(job_data).encode("utf-8"),
            Key=f"{repo_name}/_JOB_DATA_"
        )

        bucket.put_object(
            Body=json.dumps(data0).encode("utf-8"),
            Key=f"{repo_name}/file0.json"
        )

        bucket.put_object(
            Body=json.dumps(data1).encode("utf-8"),
            Key=f"{repo_name}/file1.json"
        )

        bucket.put_object(
            Body=json.dumps(data2).encode("utf-8"),
            Key=f"{repo_name}/file2.json"
        )

        bucket.put_object(
            Body=json.dumps(data3).encode("utf-8"),
            Key=f"{repo_name}/file3.json"
        )

        bucket.put_object(
            Body=json.dumps(data4).encode("utf-8"),
            Key=f"{repo_name}/file4.json"
        )

        bucket.put_object(
            Body=json.dumps(data5).encode("utf-8"),
            Key=f"{repo_name}/file5.json"
        )

        yield f"s3://{bucket_name}/{repo_name}"


@pytest.mark.parametrize("input_file", [
    "file1.json",
    "s3://repo-bucket/repo/file1.json",
])
def test_load_s3_object(input_file, mock_repo):
    result = load_s3_object(mock_repo, input_file)
    assert result == data1


def test_load_vals(mock_repo):
    inputs = json.dumps({
        "input1": "file1.json",
        "input2": "${job.file_too}.json",
        "input3": "file3.json",
    })

    expect = {
        "job": job_data["job"],
        "input1": data1,
        "input2": data2,
        "input3": data3,
    }

    result = Box(load_vals(inputs, mock_repo))

    assert isinstance(result["job"], Box)
    assert isinstance(result["input1"], Box)
    assert isinstance(result["input2"], Box)
    assert isinstance(result["input3"], BoxList)
    assert result == expect


@pytest.mark.parametrize("inputs, expect0", [
    ({"input1": "file0.json"}, data0),
    ({"input1": "file1.json"}, data1),
    ({"input1": "file4.json"}, {"input1": data4}),
    ({"input1": "file5.json"}, {"input1": data5}),
])
def test_load_vals_single_input(mock_repo, inputs, expect0):
    result = Box(load_vals(json.dumps(inputs), mock_repo))

    if inputs["input1"] == "file1.json":
        expect = {**{"job": job_data["job"]},
                  **{"input1": expect0},
                  **expect0}
        assert result == expect

        assert isinstance(result["input1"], Box)
        assert isinstance(result["a"], int)
        assert isinstance(result["b"], BoxList)
        assert isinstance(result["c"], Box)

    elif inputs["input1"] == "file0.json":
        expect = {**{"job": job_data["job"]},
                  **{"input1": expect0},
                  "x": "zee",
                  "y": "why",
                  "z": "ecks"}

        assert result == expect

    else:
        expect = {**{"job": job_data["job"]}, **expect0}
        assert str(result) == str(expect)  # ugh

        if inputs["input1"] == "file4.json":
            assert isinstance(result.input1, BoxList)
        else:
            assert isinstance(result.input1, str)


@pytest.mark.parametrize("expr, expect", [
    ("input1.a == 1", True),  # basic
    ("input2.k", True),  # boolean value
    ("input2.l", False),  # boolean value
    ("not input2.l", True),  # boolean value
    ("input1.b[1] == 3", True),  # nested list
    ("input1.c.e == 6", True),  # nested object
    ("input3[2].m.a == input1.a", True),  # compare values from different files
    # ("input2.f + input2.h.j == 1 + input3['1.2'] + input3[2].m.b", True),  # math
    ("input2.f + input2.h.j == 1 + input3[1][2] + input3[2].m.b", True),  # math
    ("7 < input2.g[1] < 10", True),  # chained
    ("input1.a == 1 and input2.f == 7", True),  # logical expression
    ("input2.g[0] != 8", False),  # false expression
    ("str(input3[1]) == '[13, 14, 15]'", True),  # built in function call
    ("abs(input1.a - 99) > 10", True),  # built in function call
    ("math.isclose(input2.f/100.0, 0.07)", True),  # math function call
    ("'u' in input3[3]", True),  # string expression
    ("input3[3].startswith('w')", True),  # string function
    ("re.match(r'wu.', input3[3]) is not None", True),  # regex
    ("'d' in input1.c", True),  # dict key lookup
    ("8 in input2.g", True),  # list lookup
])
def test_eval_this(expr, expect):
    vals = Box({
        "input1": data1,
        "input2": data2,
        "input3": data3,
    }, box_dots=True)

    result = eval_this(expr, vals)
    assert result == expect


@pytest.mark.parametrize("vals, expect", [
    ({"a": 1, "b": 2, "c": 3}, "a == 1"),
    ({"a": 9, "b": 8, "c": 3}, "c == 3"),
    ({"a": 9, "b": 8, "c": 7}, None)
])
def test_run_exprs(vals, expect):
    exprs = [
        "a == 1",
        "b == 2",
        "c == 3"
    ]
    result = run_exprs(exprs, vals)
    assert result == expect


@pytest.mark.parametrize("exprs, expect", [
    (["job.eh", "input1.a == 1", "input2.f == 7", "input3[0] == 12"], "job.eh"),
    (["not job.eh", "input1.a == 1", "input2.f == 7", "input3[0] == 12"], "input1.a == 1"),
    (["not job.eh", "input1.a > 99", "input2.f > 7", "input3[0] == 12"], "input3[0] == 12"),
    (["not job.eh", "input1.a > 99", "input2.f != 7", "input3[0] < 0"], None)
])
def test_lambda_handler_multi_expr(mock_repo, exprs, expect):
    inputs = json.dumps({
        "input1": "file1.json",
        "input2": "file2.json",
        "input3": "file3.json",
    })

    event = {
        "repo": mock_repo,
        "inputs": inputs,
        "expressions": exprs,
        "logging": {}
    }

    result = lambda_handler(event, {})
    assert result == expect


@pytest.mark.parametrize("exprs, expect", [
    (["job.eh", "job.bee.sea[1] == 'ee'"], "job.eh"),
    (["not job.eh", "job.bee.sea[1] == 'ee'"], "job.bee.sea[1] == 'ee'"),
    (["not job.eh", "job.bee.gee.aytch != 'eye'"], None),
])
def test_lambda_handler_multi_expr_job_data_only(mock_repo, exprs, expect):
    event = {
        "repo": mock_repo,
        "inputs": json.dumps({}),
        "expressions": exprs,
        "logging": {}
    }
    result = lambda_handler(event, {})
    assert result == expect


@pytest.mark.parametrize("inputs, exprs, expect", [
    ({"input1": "file1.json"}, ["a == 1", "input1.b[2] == 4"], "a == 1"),
    ({"input1": "file1.json"}, ["a > 1", "input1.b[2] == 4"], "input1.b[2] == 4"),
    ({"input1": "file4.json"}, ["input1[0] > 100", "input1[1] == 100", "input1[2] < 100"], "input1[2] < 100"),
    ({"input1": "file5.json"}, ["input1.startswith('q')", "input1.endswith('h')"], "input1.endswith('h')"),
])
def test_lambda_handler_multi_expr_single_input(mock_repo, inputs, exprs, expect):
    event = {
        "repo": mock_repo,
        "inputs": json.dumps(inputs),
        "expressions": exprs,
        "logging": {}
    }
    result = lambda_handler(event, {})
    assert result == expect


@pytest.mark.parametrize("inputs, expression", [
    ({"input1": "file1.json", "input2": "file2.json", "input3": "file3.json"}, "input1.a == 1"),
    ({}, "job.eh"),
    ({"input1": "file1.json"}, "c.d == 5"),
])
def test_lambda_handler_single_expr_succeed(mock_repo, inputs, expression):
    event = {
        "repo": mock_repo,
        "inputs": json.dumps(inputs),
        "expression": expression,
        "logging": {}
    }

    result = lambda_handler(event, {})
    assert result == expression


@pytest.mark.parametrize("inputs, expression", [
    ({"input1": "file1.json", "input2": "file2.json", "input3": "file3.json"}, "input1.a != 1"),
    ({}, "job.bee.gee.aytch != 'eye'"),
    ({"input1": "file4.json"}, "input1[1] > 100")
])
def test_lambda_handler_single_expr_fail(mock_repo, inputs, expression):
    event = {
        "repo": mock_repo,
        "inputs": json.dumps(inputs),
        "expression": expression,
        "logging": {}
    }

    with pytest.raises(ConditionFailed):
        _ = lambda_handler(event, {})
