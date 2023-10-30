from contextlib import closing
import json

import boto3
import moto
import pytest

from ...src.scatter_init.scatter_init import lambda_handler

JOB_DATA_TEMPLATE = {
    "job": {"job": "data"},
    "scatter": {},
    "parent": {"file": "s3://test-bucket/repo/path/file.txt"}
}


@pytest.fixture(scope="module")
def repo_bucket():
    with moto.mock_s3():
        yld = boto3.resource("s3", region_name="us-east-1").Bucket("test-bucket")
        yld.create()
        yld.put_object(Key="repo/path/Scatter/_JOB_DATA_", Body=json.dumps(JOB_DATA_TEMPLATE).encode("utf-8"))
        yield yld


def test_lambda_handler(repo_bucket):
    event = {
        "index": "99",
        "repo": f"s3://{repo_bucket.name}/repo/path/Scatter",
        "scatter": {
            "number": "88",
            "file": "s3://bucket/yada/yada/file.txt",
        },
        "logging": {},
    }

    result = lambda_handler(event, {})
    expect = f"s3://{repo_bucket.name}/repo/path/Scatter/00099"

    assert result == expect

    result_bucket, result_prefix = result.split("/", 3)[2:]
    job_data_obj = boto3.resource("s3").Object(result_bucket, f"{result_prefix}/_JOB_DATA_")
    response = job_data_obj.get()
    with closing(response["Body"]) as fp:
        job_data = json.load(fp)

    expected_job_data = {
        "job": JOB_DATA_TEMPLATE["job"],
        "scatter": event["scatter"],
        "parent": JOB_DATA_TEMPLATE["parent"],
    }
    assert job_data == expected_job_data
