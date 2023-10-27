import json

import pytest

from ...src.common.python.repo_utils import S3File, Repo, RepoEncoder, OtherEncoder


def test_s3file_repr():
    result = S3File("bucket", "path/to/file.txt")
    expect = "s3://bucket/path/to/file.txt"
    assert str(result) == expect


@pytest.mark.parametrize("uri, expected_bucket, expected_key", [
    ("plain_filename.txt", "repo-bucket", "repo/prefix/plain_filename.txt"),
    ("s3://other-bucket/other/dir/filename.txt", "other-bucket", "other/dir/filename.txt")
])
def test_repo_qualify(uri, expected_bucket, expected_key):
    repo = Repo("repo-bucket", "repo/prefix")
    result = repo.qualify(uri)
    assert isinstance(result, S3File)
    assert result.bucket == expected_bucket
    assert result.key == expected_key


def test_repo_sub_repo():
    repo = Repo("repo-bucket", "repo/prefix")
    result = repo.sub_repo("sub-repo")
    assert isinstance(result, Repo)
    assert result.bucket == "repo-bucket"
    assert result.prefix == "repo/prefix/sub-repo"


def test_repo_job_data_file():
    repo = Repo("repo-bucket", "repo/prefix")
    result = repo.job_data_file
    assert isinstance(result, S3File)
    assert result.bucket == "repo-bucket"
    assert result.key == "repo/prefix/_JOB_DATA_"


def test_repo_repr():
    result = Repo("repo-bucket", "repo/prefix")
    expect = "s3://repo-bucket/repo/prefix"
    assert str(result) == expect


def test_repo_encoder():
    data = {
        "repo": Repo("bucket", "path/to/repo"),
        "s3_file": S3File("bucket", "path/to/repo/file.txt"),
        "nested": {
            "repo": Repo("bucket", "path/to/another/repo"),
            "s3_file": S3File("bucket", "path/to/another/repo/another_file.txt"),
        },
        "string": "this is a string",
        "number": 99,
        "list": [1, 2, 3, 4]
    }
    result = json.dumps(data, cls=RepoEncoder, sort_keys=True)

    expect0= {
        "repo": "s3://bucket/path/to/repo",
        "s3_file": "s3://bucket/path/to/repo/file.txt",
        "nested": {
            "repo": "s3://bucket/path/to/another/repo",
            "s3_file": "s3://bucket/path/to/another/repo/another_file.txt",
        },
        "string": "this is a string",
        "number": 99,
        "list": [1, 2, 3, 4]
    }
    expect = json.dumps(expect0, sort_keys=True)
    assert result == expect


def test_other_encoder():
    data = {
        "repo": Repo("bucket", "path/to/repo"),
        "other": "stuff",
    }
    result = json.dumps(data, cls=OtherEncoder, sort_keys=True)

    expect0 = {
        "other": "stuff",
        "repo": {
            "bucket": "bucket",
            "prefix": "path/to/repo",
            "uri": "s3://bucket/path/to/repo"
        },
    }
    expect = json.dumps(expect0, sort_keys=True)
    assert result == expect