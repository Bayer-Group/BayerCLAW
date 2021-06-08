from contextlib import closing
import json
import logging
import os
import sys

import boto3
import moto
import pytest

# make common layer modules available
sys.path.append(
    os.path.realpath(
        os.path.join(
            os.path.dirname(__file__),  # (home)/lambda/tests/scatter
            os.pardir,                  # (home)/lambda/tests
            os.pardir,                  # (home)/lambda
            "src", "common", "python"
        )
    )
)

logging.basicConfig(level=logging.INFO)

from ...src.subpipes.subpipes import copy_file_impl, lambda_handler

TEST_BUCKET = "test-bucket"
JOB_DATA = {
    "job": {
        "key1": "value1",
        "key2": "value2",
        "key3": "value3"
    },
    "scatter": {"scatter": "stuff"},
    "parent": {"parent": "stuff"}
}


@pytest.fixture(scope="function")
def repo_bucket():
    with moto.mock_s3():
        yld = boto3.resource("s3", region_name="us-east-1").Bucket(TEST_BUCKET)
        yld.create()
        yield yld


@pytest.mark.parametrize("src_key, dst_key, spec", [
    ("src/path/file1.txt", "dst/path/file_one.txt", "file1.txt -> file_one.txt"),
    ("src/path/file2.txt", "dst/path/file2.txt", "file2.txt"),
])
def test_copy_file_impl(src_key, dst_key, spec, repo_bucket):
    repo_bucket.put_object(Key=src_key, Body=b"file contents")

    copy_file_impl(spec, repo_bucket.name, "src/path", "dst/path")

    expect_obj = repo_bucket.Object(dst_key)
    response = expect_obj.get()
    with closing(response["Body"]) as fp:
        lines = list(fp.iter_lines())

    assert lines[0] == b"file contents"


def test_lambda_handler_submit(repo_bucket):
    repo_bucket.put_object(Key="repo/file1.txt", Body=b"file one")
    repo_bucket.put_object(Key="repo/file2.txt", Body=b"file two")
    repo_bucket.put_object(Key="repo/value1.txt", Body=b"value one")
    repo_bucket.put_object(Key="repo/value3.txt", Body=b"value three")
    repo_bucket.put_object(Key="repo/_JOB_DATA_", Body=json.dumps(JOB_DATA).encode())

    submit = [
        "file1.txt -> fileA.txt",
        "file2.txt",
        "${job.key1}.txt -> ${job.key2}.txt",
        "${job.key3}.txt"
        ""
    ]

    event = {
        "repo": f"s3://{repo_bucket.name}/repo",
        "submit": json.dumps(submit),
        "logging": {
            "step_name": "subpipe",
        },
    }

    expected_result = {"sub_repo": f"s3://{repo_bucket.name}/repo/subpipe"}

    result = lambda_handler(event, {})
    assert result == expected_result

    obj1 = repo_bucket.Object("repo/subpipe/fileA.txt")
    response = obj1.get()
    with closing(response["Body"]) as fp:
        lines = list(fp.iter_lines())
        assert lines[0] == b"file one"

    obj2 = repo_bucket.Object("repo/subpipe/file2.txt")
    response = obj2.get()
    with closing(response["Body"]) as fp:
        lines = list(fp.iter_lines())
        assert lines[0] == b"file two"

    obj3 = repo_bucket.Object("repo/subpipe/_JOB_DATA_")
    response = obj3.get()
    with closing(response["Body"]) as fp:
        sub_job_data = json.load(fp)

    assert sub_job_data["job"] == JOB_DATA["job"]
    assert sub_job_data["parent"] == {}
    assert sub_job_data["scatter"] == {}

    obj4 = repo_bucket.Object("repo/subpipe/value2.txt")
    response = obj4.get()
    with closing(response["Body"]) as fp:
        lines = list(fp.iter_lines())
        assert lines[0] == b"value one"

    obj5 = repo_bucket.Object("repo/subpipe/value3.txt")
    response = obj5.get()
    with closing(response["Body"]) as fp:
        lines = list(fp.iter_lines())
        assert lines[0] == b"value three"


def test_lambda_handler_submit_no_files(repo_bucket):
    repo_bucket.put_object(Key="repo/_JOB_DATA_", Body=json.dumps(JOB_DATA).encode())

    event = {
        "repo": f"s3://{repo_bucket.name}/repo",
        "submit": "[]",
        "logging": {
            "step_name": "subpipe",
        },
    }

    expected_result = {"sub_repo": f"s3://{repo_bucket.name}/repo/subpipe"}

    result = lambda_handler(event, {})
    assert result == expected_result

    obj = repo_bucket.Object("repo/subpipe/_JOB_DATA_")
    response = obj.get()
    with closing(response["Body"]) as fp:
        sub_job_data = json.load(fp)

    assert sub_job_data["job"] == JOB_DATA["job"]
    assert sub_job_data["parent"] == {}
    assert sub_job_data["scatter"] == {}


def test_lambda_handler_retrieve(repo_bucket):
    repo_bucket.put_object(Key="repo/subpipe/fileA.txt", Body=b"file eh")
    repo_bucket.put_object(Key="repo/subpipe/fileB.txt", Body=b"file bee")
    repo_bucket.put_object(Key="repo/subpipe/value1.txt", Body=b"value one")
    repo_bucket.put_object(Key="repo/subpipe/value3.txt", Body=b"value three")
    repo_bucket.put_object(Key="repo/_JOB_DATA_", Body=json.dumps(JOB_DATA).encode())

    retrieve = [
        "fileA.txt -> file1.txt",
        "fileB.txt",
        "${job.key1}.txt -> ${job.key2}.txt",
        "${job.key3}.txt"
    ]

    event = {
        "repo": f"s3://{repo_bucket.name}/repo",
        "retrieve": json.dumps(retrieve),
        "subpipe": {
            "sub_repo": f"s3://{repo_bucket.name}/repo/subpipe"
        },
        "logging": {},
    }

    _ = lambda_handler(event, {})

    obj1 = repo_bucket.Object("repo/file1.txt")
    response = obj1.get()
    with closing(response["Body"]) as fp:
        lines = list(fp.iter_lines())
        assert lines[0] == b"file eh"

    obj2 = repo_bucket.Object("repo/fileB.txt")
    response = obj2.get()
    with closing(response["Body"]) as fp:
        lines = list(fp.iter_lines())
        assert lines[0] == b"file bee"

    obj3 = repo_bucket.Object("repo/value2.txt")
    response = obj3.get()
    with closing(response["Body"]) as fp:
        lines = list(fp.iter_lines())
        assert lines[0] == b"value one"

    obj4 = repo_bucket.Object("repo/value3.txt")
    response = obj4.get()
    with closing(response["Body"]) as fp:
        lines = list(fp.iter_lines())
        assert lines[0] == b"value three"


def test_lambda_handler_retrieve_no_files(repo_bucket):
    repo_bucket.put_object(Key="repo/_JOB_DATA_", Body=json.dumps(JOB_DATA).encode())

    event = {
        "repo": f"s3://{repo_bucket.name}/repo",
        "retrieve": "[]",
        "subpipe": {
            "sub_repo": f"s3://{repo_bucket.name}/repo/subpipe"
        },
        "logging": {},
    }

    _ = lambda_handler(event, {})
