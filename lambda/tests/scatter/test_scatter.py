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

from ...src.scatter.scatter import prepend_repo, expand_glob, expand_scatter_data, scatterator, lambda_handler

TEST_BUCKET = "test-bucket"
JOB_DATA = {"job": {"job": "data"}, "parent": {}, "scatter": {}}
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


@pytest.mark.parametrize("file, expect", [
    ("bare_filename.txt", "s3://path/to/repo/bare_filename.txt"),
    ("s3://path/to/repo/file_path.txt", "s3://path/to/repo/file_path.txt")
])
def test_prepend_repo(file, expect):
    result = prepend_repo(file, "s3://path/to/repo")
    assert result == expect


@pytest.mark.parametrize("glob, expect", [
    ("file*", ["file1", "file2", "file3"]),
    ("file?", ["file1", "file2", "file3"]),
    ("file[12]", ["file1", "file2"]),
    ("*file*", ["file1", "file2", "file3", "other_file.json"]),
    ("nothing*", []),
])
def test_expand_glob(repo_bucket, glob, expect):
    repo = f"s3://{repo_bucket.name}/repo/path"
    result = sorted(expand_glob(glob, repo))
    ext_expect = [f"{repo}/{x}" for x in expect]
    assert result == ext_expect


def test_expand_scatter_data(repo_bucket):
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

    expect = {
        "static_list": [1, 2, 3],
        "job_data_list": [9, 8, 7],
        "file_contents": ["2", "4"],  # why are these stringified?
        "file_glob": [
            f"s3://{repo_bucket.name}/repo/path/file1",
            f"s3://{repo_bucket.name}/repo/path/file2",
            f"s3://{repo_bucket.name}/repo/path/file3",
        ],
        "single_file": [f"s3://{repo_bucket.name}/repo/path/single.txt"]
    }

    result = expand_scatter_data(scatter_spec, f"s3://{repo_bucket.name}/repo/path", job_data)
    assert result == expect


def test_expand_scatter_data_not_list():
    scatter_spec = {"unlist": "${job.unlist}"}
    job_data = {"job": {"unlist": 99}, "parent": {}, "scatter": {}}

    with pytest.raises(RuntimeError, match="'job.unlist' is not a JSON list"):
        expand_scatter_data(scatter_spec=scatter_spec, repo="s3://bucket/whatever", job_data=job_data)


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


def test_lambda_handler(repo_bucket, caplog):
    parent_repo = f"s3://{repo_bucket.name}/repo/path"
    event = {
        "repo": parent_repo,
        "scatter": json.dumps({"scatter_files": "file*", "list": [1, 2]}),
        "inputs": json.dumps({"other_file": "other_file.json"}),
        "logging": {
            "step_name": "test_step",
        },
    }

    expected_scatters = [
        ("file1", 1),
        ("file1", 2),
        ("file2", 1),
        ("file2", 2),
        ("file3", 1),
        ("file3", 2),
    ]

    result = lambda_handler(event, {})

    assert isinstance(result, list)
    assert len(result) == 6

    for entry, expected_scatter in zip(result, expected_scatters):
        assert isinstance(entry, dict)
        assert list(entry.keys()) == ["repo"]
        subrepo_path = entry["repo"].split("/", 3)[-1]

        sub_job_data_s3 = repo_bucket.Object(f"{subrepo_path}/_JOB_DATA_")
        response = sub_job_data_s3.get()
        with closing(response["Body"]) as fp:
            job_data = json.load(fp)

        expect = {
            "job": {
                "job": "data",
            },
            "scatter": {
                "scatter_files": f"{parent_repo}/{expected_scatter[0]}",
                "list": expected_scatter[1],
            },
            "parent": {
                "other_file": f"{parent_repo}/other_file.json",
            },
        }

        assert job_data == expect
