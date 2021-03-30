from contextlib import closing
import json
import logging
import os
import sys

import boto3
from moto import mock_s3
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

from ...src.launcher.launcher import read_s3_object, substitute_job_data, copy_job_data_to_repo, \
    write_extended_job_data_object, write_execution_record, lambda_handler


@pytest.fixture(scope="function")
def launcher_bucket():
    with mock_s3():
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


def test_substitute_job_data():
    target = "s3://${job.bucket}/${!job.path1}/${job.path2.path3}/${job.path4[0]}"
    job_data = {
        "bucket": "bucket-name",
        "path1": "path-one",
        "path2": {
            "path3": "path-two-three",
            "pathx": "don't go there",
        },
        "path4": [
            "path-four",
            "don't go here either"
        ]
    }
    result = substitute_job_data(target, job_data)
    expect = "s3://bucket-name/path-one/path-two-three/path-four"
    assert result == expect

    bad_target = "s3://${job.bucket}/${job.path99}/"
    with pytest.raises(RuntimeError, match=r"unrecognized job data field in .*"):
        substitute_job_data(bad_target, job_data)


@mock_s3
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


def test_lambda_handler(monkeypatch, caplog, launcher_bucket):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

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
        "repo_template": "s3://repo-bucket/path/to/repo/${job.name}",
        "input_obj": {
            "job_file": {
                "bucket": launcher_bucket,
                "key": job_data_key,
                "version": version,
                "s3_request_id": "ELVISLIVES",
            },
            "index": "main",
        },
        "logging": {
            "branch": "main",
            "job_file_bucket": launcher_bucket,
            "job_file_key": job_data_key,
            "job_file_version": version,
            "job_file_s3_request_id": "ELVISLIVES",
            "sfn_execution_id": "test-execution-id",
            "step_name": "test-step",
            "workflow_name": "test-workflow",
        },
    }

    result = lambda_handler(event, {})

    # return value to send to step functions
    expect = {
        "index": "main",
        "id_prefix": "test",
        "job_file": {
            "bucket": launcher_bucket,
            "key": job_data_key,
            "version": version,
            "s3_request_id": "ELVISLIVES",
        },
        "repo": "s3://repo-bucket/path/to/repo/testJob",
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

    # execution record written to repo
    execution_record_obj = conn.Object("repo-bucket", "path/to/repo/testJob/execution_info/test-execution-id").get()
    with closing(execution_record_obj["Body"]) as fp:
        execution_record = json.load(fp)
    assert execution_record == event


def test_lambda_handler_subpipe_execution(caplog):
    input_obj = {
        "index": "main",
        "id_prefix": "testPrefix",
        "job_file": {
            "bucket": "testBucket",
            "key": "path/to/job/file.txt",
            "version": "testVersion",
            "s3_request_id": "ELVISLIVES",
        },
        "AWS_STEP_FUNCTIONS_STARTED_BY_EXECUTION_ID": "1234567890",
        "repo": "path/to/sub/repo"
    }

    event = {
        "input_obj": input_obj,
        "logging": {
            "branch": "main",
            "job_file_bucket": "testBucket",
            "job_file_key": "path/to/job_file",
            "job_file_version": "testVersion",
            "job_file_s3_request_id": "ELVISLIVES",
            "sfn_execution_id": "test_execution_id",
            "step_name": "test-step",
            "workflow_name": "test-workflow",
        },
    }

    result = lambda_handler(event, {})
    assert result == input_obj
