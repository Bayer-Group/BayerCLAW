from contextlib import closing
import json
import logging

import boto3
import moto
import pytest

from ...src.gather.gather import lambda_handler

logging.basicConfig(level=logging.INFO)

TEST_BUCKET = "test-bucket"
JOB_DATA = {"job": {"job": "data"}, "parent": {}, "scatter": {}}


@pytest.fixture(scope="module")
def repo_bucket():
    with moto.mock_s3():
        yld = boto3.resource("s3", region_name="us-east-1").Bucket(TEST_BUCKET)
        yld.create()
        yld.put_object(Key="repo/path/_JOB_DATA_", Body=json.dumps(JOB_DATA).encode("utf-8"))

        yld.put_object(Key="repo/path/test-step/00000/output1", Body=b"00000.output1")
        yld.put_object(Key="repo/path/test-step/00000/output2", Body=b"00000.output2")
        yld.put_object(Key="repo/path/test-step/00000/zoutput2", Body=b"00000.zoutput2")
        yld.put_object(Key="repo/path/test-step/00000/unoutput", Body=b"00000.unoutput")

        yld.put_object(Key="repo/path/test-step/00001/output1", Body=b"00001.output1")
        # no output2 in subdir 00001
        yld.put_object(Key="repo/path/test-step/00001/unoutput", Body=b"00001.unoutput")

        yld.put_object(Key="repo/path/test-step/00002/output1", Body=b"00002.output1")
        yld.put_object(Key="repo/path/test-step/00002/output2", Body=b"00002.output2")
        yld.put_object(Key="repo/path/test-step/00002/unoutput", Body=b"00002.unoutput")

        yield yld


def test_lambda_handler(caplog, repo_bucket):
    event = {
        "repo": f"s3://{repo_bucket.name}/repo/path",
        "outputs": json.dumps({"out1": "output1", "out2": "output2", "out3": "output3"}),
        "items": [
            {"repo": f"s3://{repo_bucket.name}/repo/path/test-step/00000"},
            {"repo": f"s3://{repo_bucket.name}/repo/path/test-step/00001"},
            {"repo": f"s3://{repo_bucket.name}/repo/path/test-step/00002"},
        ],
        "logging": {
            "step_name": "test-step",
        },
    }

    expect = {"manifest": "test-step_manifest.json"}
    result = lambda_handler(event, {})
    assert result == expect

    manifest_key = f"repo/path/{result['manifest']}"
    manifest_s3 = repo_bucket.Object(manifest_key)
    response = manifest_s3.get()
    with closing(response["Body"]) as fp:
        manifest = json.load(fp)

    expect = {
        "out1": [
            f"s3://{repo_bucket.name}/repo/path/test-step/00000/output1",
            f"s3://{repo_bucket.name}/repo/path/test-step/00001/output1",
            f"s3://{repo_bucket.name}/repo/path/test-step/00002/output1",
        ],
        "out2": [
            f"s3://{repo_bucket.name}/repo/path/test-step/00000/output2",
            f"s3://{repo_bucket.name}/repo/path/test-step/00002/output2",
        ],
        "out3": [],
    }
    assert manifest == expect

    assert "no files named output3 found" in caplog.text


def test_lambda_handler_no_manifest(caplog, repo_bucket):
    event = {
        "repo": f"s3://{repo_bucket.name}/repo/path",
        "outputs": "{}",
        "results": ["fake", "results"],
        "logging": {
            "step_name": "test-step",
        },
    }

    result = lambda_handler(event, {})
    assert result == {}
