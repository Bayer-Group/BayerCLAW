import fcntl
import logging
import os

import boto3
import moto
import pytest

logging.basicConfig(level=logging.INFO)

from ..src.runner.cache import _blocking_download, _download_to_cache, get_reference_inputs

TEST_BUCKET = "test-bucket"
FILE1_CONTENT = "file one"
FILE2_CONTENT = "file two"
FILE3_CONTENT = "file three"


@pytest.fixture(scope="module")
def s3_bucket():
    with moto.mock_s3():
        boto3.client("s3").create_bucket(Bucket=TEST_BUCKET)
        yld = boto3.resource("s3").Bucket(TEST_BUCKET)
        yld.put_object(Key="some/path/file1", Body=FILE1_CONTENT.encode("utf-8"))
        yld.put_object(Key="other/path/file2", Body=FILE2_CONTENT.encode("utf-8"))
        yld.put_object(Key="one/more/path/file3", Body=FILE3_CONTENT.encode("utf-8"))
        yield yld


def test_blocking_download(tmp_path, s3_bucket):
    src = s3_bucket.Object("some/path/file1")
    dst = f"{tmp_path}/file1"
    _blocking_download(src, dst, "file1")
    assert os.path.isfile(dst)


def test_blocking_download_already_there(tmp_path, caplog):
    caplog.set_level(logging.INFO)
    dst = f"{tmp_path}/file99"
    open(dst, "w").close()
    _blocking_download("s3://does/not/exist", dst, "file99")
    assert "found file99 in cache" in caplog.text


def test_blocking_download_blocked(tmp_path, s3_bucket):
    src = s3_bucket.Object("some/path/file1")
    dst = f"{tmp_path}/file1"
    lock_file = f"{os.path.dirname(dst)}.lock"
    with open(lock_file, "w") as lfp:
        fcntl.flock(lfp, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            _blocking_download(src, dst, "file1")
    os.remove(lock_file)


def test_download_to_cache(monkeypatch, tmp_path, s3_bucket):
    monkeypatch.setenv("BC_SCRATCH_PATH", str(tmp_path))

    src_etag = s3_bucket.Object("some/path/file1").e_tag.strip('"')

    result = _download_to_cache(("test_file", f"s3://{TEST_BUCKET}/some/path/file1"))
    expected = "test_file", f"{tmp_path}/{src_etag}/file1"
    assert result == expected

    key, cached_file = result
    assert os.path.isfile(cached_file)
    with open(cached_file) as fp:
        cached_content = fp.readline()
        assert cached_content == FILE1_CONTENT

    with open(cached_file, "a") as fp:
        print("extra content", file=fp)

    result2 = _download_to_cache(("test_file", f"s3://{TEST_BUCKET}/some/path/file1"))
    _, cached_file2 = result2
    with open(cached_file2) as fp2:
        cached_content2 = fp2.readline()
        assert cached_content2 == FILE1_CONTENT + "extra content\n"


def test_get_reference_inputs(monkeypatch, tmp_path, s3_bucket):
    monkeypatch.setenv("BC_SCRATCH_PATH", str(tmp_path))

    ref_spec = {
        "file1": f"s3://{TEST_BUCKET}/some/path/file1",
        "file2": f"s3://{TEST_BUCKET}/other/path/file2",
        "file3": f"s3://{TEST_BUCKET}/one/more/path/file3",
    }

    workspace = f"{str(tmp_path)}/workdir"
    os.makedirs(workspace)
    os.chdir(workspace)

    result = get_reference_inputs(ref_spec)
    expect = {
        "file1": "file1",
        "file2": "file2",
        "file3": "file3",
    }
    assert result == expect

    for file, expected_content in {"file1": FILE1_CONTENT, "file2": FILE2_CONTENT, "file3": FILE3_CONTENT}.items():
        assert os.path.isfile(file)
        assert os.stat(file).st_nlink >= 2  # make sure the file is a hard link
        with open(file) as fp:
            content = fp.readline()
            assert content == expected_content


def test_get_reference_inputs_fail(monkeypatch, tmp_path, s3_bucket):
    monkeypatch.setenv("BC_SCRATCH_PATH", str(tmp_path))

    ref_spec = {
        "file1": f"s3://{TEST_BUCKET}/some/path/file1",
        "file2": f"s3://{TEST_BUCKET}/other/path/file2",
        "file3": f"s3://{TEST_BUCKET}/does/not/exist/file99",
    }

    workspace = f"{str(tmp_path)}/workdir"
    os.makedirs(workspace)
    os.chdir(workspace)

    with pytest.raises(Exception):
        _ = get_reference_inputs(ref_spec)
