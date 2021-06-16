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

from ...src.gather.gather import _output_path_generator, find_output_files, lambda_handler

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
        yld.put_object(Key="repo/path/test-step/00000/unoutput", Body=b"00000.unoutput")

        yld.put_object(Key="repo/path/test-step/00001/output1", Body=b"00001.output1")
        # no output2 in subdir 00001
        yld.put_object(Key="repo/path/test-step/00001/unoutput", Body=b"00001.unoutput")

        yld.put_object(Key="repo/path/test-step/00002/output1", Body=b"00002.output1")
        yld.put_object(Key="repo/path/test-step/00002/output2", Body=b"00002.output2")
        yld.put_object(Key="repo/path/test-step/00002/unoutput", Body=b"00002.unoutput")

        yield yld


@pytest.mark.parametrize("filename, expect", [
    ("output1", [f"s3://{TEST_BUCKET}/repo/path/test-step/00000/output1",
                 f"s3://{TEST_BUCKET}/repo/path/test-step/00001/output1",
                 f"s3://{TEST_BUCKET}/repo/path/test-step/00002/output1"]),
    ("output2", [f"s3://{TEST_BUCKET}/repo/path/test-step/00000/output2",
                 f"s3://{TEST_BUCKET}/repo/path/test-step/00002/output2"]),
    ("output3", [])
])
def test_output_path_generator(caplog, repo_bucket, filename, expect):
    repos = [
        f"s3://{repo_bucket.name}/repo/path/test-step/00000",
        f"s3://{repo_bucket.name}/repo/path/test-step/00001",
        f"s3://{repo_bucket.name}/repo/path/test-step/00002",
    ]
    result = sorted(list(_output_path_generator(filename, repos)))
    assert result == expect

    if filename != "output1":
        assert f"{filename} not found in" in caplog.text


@pytest.mark.parametrize("key, filename, expect", [
    ("key1", "output1", [f"s3://{TEST_BUCKET}/repo/path/test-step/00000/output1",
                         f"s3://{TEST_BUCKET}/repo/path/test-step/00001/output1",
                         f"s3://{TEST_BUCKET}/repo/path/test-step/00002/output1"]),
    ("key2", "output2", [f"s3://{TEST_BUCKET}/repo/path/test-step/00000/output2",
                         f"s3://{TEST_BUCKET}/repo/path/test-step/00002/output2"]),
    ("key3", "output3", [])
])
def test_find_output_files(caplog, repo_bucket, key, filename, expect):
    repos = [
        f"s3://{repo_bucket.name}/repo/path/test-step/00000",
        f"s3://{repo_bucket.name}/repo/path/test-step/00001",
        f"s3://{repo_bucket.name}/repo/path/test-step/00002",
    ]
    result_key, result_list = find_output_files((key, filename), repos)
    assert result_key == key
    assert sorted(result_list) == expect

    if key == "key3":
        assert f"no files named {filename} found" in caplog.text


def test_lambda_handler(caplog, repo_bucket):
    event = {
        "repo": f"s3://{repo_bucket.name}/repo/path",
        "outputs": json.dumps({"out1": "output1", "out2": "output2", "out3": "output3"}),
        "results": [
            {
                "repo": f"s3://{repo_bucket.name}/repo/path/test-step/00000",
                "other_stuff": "",
            },
            {
                "repo": f"s3://{repo_bucket.name}/repo/path/test-step/00001",
                "other_stuff": "",
            },
            {
                "repo": f"s3://{repo_bucket.name}/repo/path/test-step/00002",
                "other_stuff": "",
            },
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
