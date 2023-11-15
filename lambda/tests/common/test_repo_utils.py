import json

import pytest

from ...src.common.python.repo_utils import S3File, Repo


def test_s3file():
    result = S3File("bucket", "path/to/file.txt")
    assert result.bucket == "bucket"
    assert result.key == "path/to/file.txt"
    assert result == "s3://bucket/path/to/file.txt"


def test_s3file_json_serialize():
    s3file = S3File("bucket", "path/to/file.txt")
    result = json.dumps({"file": s3file})
    expect = '{"file": "s3://bucket/path/to/file.txt"}'
    assert result == expect


@pytest.mark.parametrize("repo_spec, expected_uri", [
    ({"bucket": "repo-bucket", "prefix": "path/to/repo"}, "s3://repo-bucket/path/to/repo"),
    ({"bucket": "repo-bucket", "prefix": "path/to/repo", "uri": "s3://just/for/testing"}, "s3://just/for/testing")
])
def test_repo_init(repo_spec, expected_uri):
    result = Repo(repo_spec)
    assert result.bucket == repo_spec["bucket"]
    assert result.prefix == repo_spec["prefix"]
    assert result.uri == expected_uri


def test_repo_from_uri():
    uri = "s3://repo-bucket/path/to/repo"
    result = Repo.from_uri(uri)
    assert result.bucket == "repo-bucket"
    assert result.prefix == "path/to/repo"


@pytest.mark.parametrize("spec, expected_bucket, expected_key", [
    ("plain_filename.txt", "repo-bucket", "repo/prefix/plain_filename.txt"),
    ("s3://other-bucket/other/dir/filename.txt", "other-bucket", "other/dir/filename.txt")
])
def test_repo_qualify(spec, expected_bucket, expected_key):
    repo = Repo(bucket="repo-bucket", prefix="repo/prefix")
    result = repo.qualify(spec)
    assert isinstance(result, S3File)
    assert result.bucket == expected_bucket
    assert result.key == expected_key


def test_repo_sub_repo():
    repo = Repo(bucket="repo-bucket", prefix="repo/prefix")
    result = repo.sub_repo("sub-repo")
    assert isinstance(result, Repo)
    assert result.bucket == "repo-bucket"
    assert result.prefix == "repo/prefix/sub-repo"


def test_repo_repr():
    result = Repo(bucket="repo-bucket", prefix="repo/prefix")
    expect = "s3://repo-bucket/repo/prefix"
    assert str(result) == expect


def test_repo_json_serialize():
    repo = Repo(bucket="repo-bucket", prefix="repo/prefix")
    result = json.dumps({"repo": repo}, sort_keys=True)
    expect0 = {
        "repo": {
            "bucket": "repo-bucket",
            "prefix": "repo/prefix",
            "uri": "s3://repo-bucket/repo/prefix"
        }
    }
    expect = json.dumps(expect0, sort_keys=True)
    assert result == expect


def test_repo_to_dict():
    repo = Repo(bucket="repo-bucket", prefix="repo/prefix")
    result = dict(repo)
    expect = {
        "bucket": "repo-bucket",
        "prefix": "repo/prefix",
        "uri": "s3://repo-bucket/repo/prefix"
    }
    assert result == expect