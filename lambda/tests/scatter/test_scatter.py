from contextlib import closing
import csv
import json
import re

import boto3
import moto
import pytest

from ...src.scatter.scatter import (get_job_data, expand_glob, expand_scatter_data, scatterator,
                                    write_job_data_template, lambda_handler)
# https://stackoverflow.com/a/46709888
from repo_utils import Repo, S3File

TEST_BUCKET = "test-bucket"
JOB_DATA = {
    "job": {
        "job": "data",
        "glob": f"s3://{TEST_BUCKET}/repo/path/file*",
    },
    "parent": {},
    "scatter": {}
}
FILE1_CONTENT = "file one"
FILE2_CONTENT = "file two"
FILE3_CONTENT = "file three"
OTHER_FILE_CONTENT = json.dumps({"x": {"a": 1, "b": 2},
                                 "y": {"a": 3, "b": 4}})


@pytest.fixture(scope="module")
def repo_bucket():
    with moto.mock_s3():
        yld = boto3.resource("s3", region_name="us-east-1").Bucket(TEST_BUCKET)
        yld.create()
        yld.put_object(Key="repo/path/_JOB_DATA_", Body=json.dumps(JOB_DATA).encode("utf-8"))
        yld.put_object(Key="repo/path/file1", Body=FILE1_CONTENT.encode("utf-8"))
        yld.put_object(Key="repo/path/file2", Body=FILE2_CONTENT.encode("utf-8"))
        yld.put_object(Key="repo/path/file3", Body=FILE3_CONTENT.encode("utf-8"))
        yld.put_object(Key="repo/path/other_file.json", Body=OTHER_FILE_CONTENT.encode("utf-8"))
        yld.put_object(Key="repo/path/_control_/test_step.complete", Body=b"")
        yield yld


def test_get_job_data(repo_bucket):
    repo = Repo(bucket=repo_bucket.name, prefix="repo/path")
    result = get_job_data(repo)
    assert result == JOB_DATA


# todo: need more?
@pytest.mark.parametrize("glob, expect", [
    ("file*", ["file1", "file2", "file3"]),
    ("file?", ["file1", "file2", "file3"]),
    ("file[12]", ["file1", "file2"]),
    ("*file*", ["file1", "file2", "file3", "other_file.json"]),
    ("nothing*", []),
])
def test_expand_glob(repo_bucket, glob, expect):
    s3_glob = S3File(repo_bucket.name, f"repo/path/{glob}")
    expanded = list(expand_glob(s3_glob))
    result = sorted(str(f) for f in expanded)
    ext_expect = [f"s3://{repo_bucket.name}/repo/path/{x}" for x in expect]
    assert result == ext_expect


def test_expand_scatter_data(repo_bucket):
    repo = Repo(bucket=repo_bucket.name, prefix="repo/path")

    scatter_spec = {
        "static_list": [1, 2, 3],
        "job_data_list": "${job.a_list}",
        "file_contents": "@other_file.json:$[*.b]",
        "file_glob": "file*",
        "single_file": "single.txt"
    }

    job_data = {
        "job": {
            "a_list": [9, 8, 7],
            "more": "stuff",
        },
        "parent": {},
        "scatter": {},
    }

    result = dict(expand_scatter_data(scatter_spec, repo, job_data))

    expect = {
        "static_list": [1, 2, 3],
        "job_data_list": [9, 8, 7],
        "file_contents": ["2", "4"],  # todo: why are these stringified?
        "file_glob": [
            S3File(repo_bucket.name, "repo/path/file1"),
            S3File(repo_bucket.name, "repo/path/file2"),
            S3File(repo_bucket.name, "repo/path/file3"),
        ],
        "single_file": [S3File(repo_bucket.name, "repo/path/single.txt")]
    }

    assert result == expect


def test_expand_scatter_data_not_list():
    scatter_spec = {"unlist": "${job.unlist}"}
    job_data = {"job": {"unlist": 99}, "parent": {}, "scatter": {}}
    repo = Repo(bucket="what", prefix="ever")

    with pytest.raises(RuntimeError, match="'job.unlist' is not a JSON list"):
        _ = list(expand_scatter_data(scatter_spec=scatter_spec, repo=repo, job_data=job_data))


def test_scatterator():
    scatter_data = {
        "one": ["a", "b"],
        "two": [1, 2, 3]
    }

    expect = [
        {"one": "a", "two": 1},
        {"one": "a", "two": 2},
        {"one": "a", "two": 3},
        {"one": "b", "two": 1},
        {"one": "b", "two": 2},
        {"one": "b", "two": 3},
    ]

    result = list(scatterator(scatter_data))
    assert result == expect


def test_write_job_data_template(repo_bucket):
    scatter_repo = Repo(bucket=repo_bucket.name, prefix="repo/path/Scatter")

    parent_job_data = {
        "job": {
            "job": "data"
        },
        "scatter": {
            "should": "be overwritten"
        },
        "parent": {
            "original": f"s3://{repo_bucket.name}/repo/path/file1"
        }
    }

    repoized_inputs = {
        "additional": S3File(repo_bucket.name, "repo/path/file2")
    }

    result = write_job_data_template(parent_job_data,
                                     repoized_inputs,
                                     scatter_repo)
    assert isinstance(result, S3File)
    assert result.bucket == repo_bucket.name
    assert result.key == "repo/path/Scatter/_JOB_DATA_"

    template_obj = boto3.resource("s3").Object(result.bucket, result.key)
    response = template_obj.get()
    with closing(response["Body"]) as fp:
        template = json.load(fp)

    expected_template = {
        "job": {
            "job": "data",
        },
        "scatter": {},
        "parent": {
            "original": f"s3://{repo_bucket.name}/repo/path/file1",
            "additional": f"s3://{repo_bucket.name}/repo/path/file2",
        },
    }

    assert template == expected_template


def test_lambda_handler(repo_bucket):
    event = {
        # "repo": f"s3://{repo_bucket.name}/repo/path",
        "repo": {
            "bucket": repo_bucket.name,
            "prefix": "repo/path",
            "uri": "s3://this/is/not/used"
        },
        "scatter": json.dumps({"scatter_files": "file*", "list": [1, 2]}),
        "inputs": json.dumps({"other_file": "other_file.json"}),
        "logging": {
            "step_name": "test_step",
        },
    }

    result = lambda_handler(event, {})
    expect = {
        "items": {
            "bucket": repo_bucket.name,
            "key": "repo/path/test_step/items.csv",
        },
        "repo": {
            "bucket": repo_bucket.name,
            "prefix": "repo/path/test_step",
            "uri": f"s3://{repo_bucket.name}/repo/path/test_step",
        },
    }
    assert result == expect

    items_obj = boto3.resource("s3").Object(result["items"]["bucket"], result["items"]["key"])
    response = items_obj.get()
    lines = response["Body"].read().decode("utf-8").splitlines(True)
    records = csv.reader(lines)

    header = next(records)
    assert header == ["scatter_files", "list"]

    for record in records:
        assert re.match("^s3://test-bucket/repo/path/file[123]$", record[0])
        assert record[1] in {"1", "2"}

    template_obj = boto3.resource("s3").Object(result["repo"]["bucket"], f"{result['repo']['prefix']}/_JOB_DATA_")
    template_obj.load()


def test_lambda_handler_scatter_sub(repo_bucket):
    event = {
        # "repo": f"s3://{repo_bucket.name}/repo/path",
        "repo": {
            "bucket": repo_bucket.name,
            "prefix": "repo/path",
            "uri": "s3://this/is/not/used"
        },
        "scatter": json.dumps({"scatter_glob": "${job.glob}"}),
        "inputs": "{}",
        "logging": {
            "step_name": "test_step",
        },
    }
    result = lambda_handler(event, {})

    items_obj = boto3.resource("s3").Object(result["items"]["bucket"], result["items"]["key"])
    response = items_obj.get()
    lines = response["Body"].read().decode("utf-8").splitlines(True)
    records = csv.reader(lines)

    header = next(records)
    assert header == ["scatter_glob"]

    for record in records:
        assert re.match("^s3://test-bucket/repo/path/file[123]$", record[0])
