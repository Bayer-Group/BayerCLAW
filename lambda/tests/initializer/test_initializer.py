from contextlib import closing
import json
import logging
import re

import boto3
import moto
import pytest

from ...src.initializer.initializer import read_s3_object, lookup, substitute_job_data, check_recursive_launch, \
    write_extended_job_data_object, lambda_handler

logging.basicConfig(level=logging.INFO)


@pytest.fixture(scope="function")
def launcher_bucket():
    with moto.mock_aws():
        bucket_name = "launcher"
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=bucket_name)
        bucket_versioning = conn.BucketVersioning(bucket_name)
        bucket_versioning.enable()
        yield bucket_name


def test_read_s3_object(monkeypatch, launcher_bucket):
    # parameter values
    fake_s3_key = "path/fake-test.txt"
    fake_s3_contents1 = {"path": "test_data"}
    fake_s3_contents2 = {"more": "fake_data"}

    # Create a fake s3 bucket and insert a fake key
    conn = boto3.resource("s3", region_name="us-east-1")

    s3obj = conn.Object(launcher_bucket, fake_s3_key)

    response1 = s3obj.put(Body=json.dumps(fake_s3_contents1).encode("utf-8"))
    version1 = response1["VersionId"]
    response2 = s3obj.put(Body=json.dumps(fake_s3_contents2).encode("utf-8"))
    version2 = response2["VersionId"]

    result1 = read_s3_object(bucket=launcher_bucket, key=fake_s3_key, version=version1)
    assert (result1 == fake_s3_contents1)
    result2 = read_s3_object(bucket=launcher_bucket, key=fake_s3_key, version=version2)
    assert (result2 == fake_s3_contents2)


@pytest.mark.parametrize("pattern, string, expect", [
    (r"(one)", "one", "wun"),
    (r"(two)", "two", "2"),
    (r"(three)", "three", ""),
    (r"(four)", "four", "False"),
])
def test_lookup(pattern, string, expect):
    spec = {
        "one": "wun",
        "two": 2,
        "three": "",
        "four": False,
    }
    match = re.match(pattern, string)
    result = lookup(match, spec)
    assert isinstance(result, str)
    assert result == expect


def test_lookup_fail():
    match = re.match(r"(unfield)", "unfield")
    with pytest.raises(KeyError, match="'unfield' not found"):
        _ = lookup(match, {"nothing": "here"})


def test_substitute_job_data():
    target = "s3://${job.bucket}/${!job.path1}/${job.path2.path3}/${job.path4[0]}/${job.path5}"
    job_data = {
        "bucket": "bucket-name",
        "path1": "path-one",
        "path2": {
            "path3": "path-two-three",
            "pathx": "don't go there",
        },
        "path4": [
            "path-four",
            "don't go here either",
        ],
        "path5": False,
    }
    result = substitute_job_data(target, job_data)
    expect = "s3://bucket-name/path-one/path-two-three/path-four/False"
    assert result == expect

    bad_target = "s3://${job.bucket}/${job.path99}/"
    with pytest.raises(KeyError, match=r"'path99' not found in job data"):
        substitute_job_data(bad_target, job_data)


@moto.mock_aws
def test_write_extended_job_data_object():
    bucket_name = "test-bucket"
    repo_path = "path/to/test/repo"
    s3 = boto3.resource("s3", region_name="us-east-1")
    bucket = s3.Bucket(bucket_name)
    bucket.create()

    job_data = {"elvis": "lives"}
    write_extended_job_data_object(job_data, bucket_name, repo_path)

    result_object = bucket.Object(f"{repo_path}/_JOB_DATA_")
    response = result_object.get()
    with closing(response["Body"]) as fp:
        result = json.load(fp)

    expect = {
        "job": job_data,
        "scatter": {},
        "parent": {},
    }
    assert result == expect


@pytest.mark.parametrize("repo_bucket, repo_path, expect_fail", [
    ("repo_bucket", "any/path", False),
    ("launcher_bucket", "different/path", False),
    ("launcher_bucket", "launcher_like/path", False),
    ("launcher_bucket", "laun/cher_like/path", False),
    ("launcher_bucket", "launcher", True),
    ("launcher_bucket", "launcher/path", True),
    ("launcher_bucket", "launcher/different/sub/path", True),
])
def test_check_recursive_launch(repo_bucket, repo_path, expect_fail):
    if expect_fail:
        with pytest.raises(RuntimeError, match="repo cannot be in the launcher folder"):
            check_recursive_launch("launcher_bucket", "launcher/path", repo_bucket, repo_path)
    else:
        check_recursive_launch("launcher_bucket", "launcher/path", repo_bucket, repo_path)


def test_lambda_handler(monkeypatch, caplog, launcher_bucket):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("BCLAW_VERSION", "test-version")

    job_data_key = "test/job.json"
    job_data = {
        "name": "testJob",
        "other_stuff": ["yada", "yada"],
    }

    conn = boto3.resource("s3", region_name="us-east-1")
    s3obj = conn.Object(launcher_bucket, job_data_key)
    response = s3obj.put(Body=json.dumps(job_data).encode("utf-8"))
    version = response["VersionId"]

    conn.create_bucket(Bucket="repo-bucket")

    event = {
        "workflow_name": "test-workflow",
        "repo_template": "s3://repo-bucket/path/to/repo/${job.name}",
        "input_obj": {
            "job_file": {
                "bucket": launcher_bucket,
                "key": job_data_key,
                "version": version,
            },
            "index": "main",
        },
        "logging": {
            "branch": "main",
            "job_file_bucket": launcher_bucket,
            "job_file_key": job_data_key,
            "job_file_version": version,
            "sfn_execution_id": "test-execution-id",
            "step_name": "test-step",
            "workflow_name": "test-workflow",
        },
    }

    result = lambda_handler(event, {})

    # return value to send to step functions
    expect = {
        "index": "main",
        "job_file": {
            "bucket": launcher_bucket,
            "key": job_data_key,
            "version": version,
        },
        "prev_outputs": {},
        "repo": {
            "bucket": "repo-bucket",
            "prefix": "path/to/repo/testJob",
            "uri": "s3://repo-bucket/path/to/repo/testJob",
        },
        "share_id": "testworkflow",
    }

    assert result == expect

    # extended job data file in repo
    ext_job_data_obj = conn.Object("repo-bucket", "path/to/repo/testJob/_JOB_DATA_").get()
    with closing(ext_job_data_obj["Body"]) as fp:
        ext_job_data = json.load(fp)
    expected_ext_job_data = {
        "job": job_data,
        "scatter": {},
        "parent": {},
    }
    assert ext_job_data == expected_ext_job_data

    # job data file copied to repo
    job_data_copy_obj = conn.Object("repo-bucket", "path/to/repo/testJob/job.json").get()
    with closing(job_data_copy_obj["Body"]) as fp:
        job_data_copy = json.load(fp)
    assert job_data_copy == job_data


def test_lambda_handler_subpipe_execution(caplog, monkeypatch):
    monkeypatch.setenv("BCLAW_VERSION", "test-version")

    input_obj = {
        "index": "main",
        "job_file": {
            "bucket": "testBucket",
            "key": "path/to/job/file.txt",
            "version": "testVersion",
        },
        "AWS_STEP_FUNCTIONS_STARTED_BY_EXECUTION_ID": "1234567890",
        "repo": "path/to/sub/repo"
    }

    event = {
        "workflow_name": "test-workflow",
        "input_obj": input_obj,
        "logging": {
            "branch": "main",
            "job_file_bucket": "testBucket",
            "job_file_key": "path/to/job_file",
            "job_file_version": "testVersion",
            "sfn_execution_id": "test_execution_id",
            "step_name": "test-step",
            "workflow_name": "test-workflow",
        },
    }

    result = lambda_handler(event, {})
    assert result == input_obj
