from contextlib import closing
import jmespath
import json
import os

import boto3
import moto
import pytest

from ..src.runner.repo import _is_glob, _s3_file_exists, _expand_s3_glob, _inputerator, _download_this, _outputerator, \
    _upload_that, Repository

TEST_BUCKET = "test-bucket"
JOB_DATA = {"job": "data"}
FILE1_CONTENT = "file one"
FILE2_CONTENT = "file two"
FILE3_CONTENT = "file three"
OTHER_FILE_CONTENT = "other file"

DIFFERENT_BUCKET = "different-bucket"
DIFFERENT_FILE_CONTENT = "different file"


@pytest.fixture(scope="module")
def repo_bucket():
    with moto.mock_s3():
        yld = boto3.resource("s3", region_name="us-east-1").Bucket(TEST_BUCKET)
        yld.create()
        yld.put_object(Key="repo/path/_JOB_DATA_", Body=json.dumps(JOB_DATA).encode("utf-8"))
        yld.put_object(Key="repo/path/file1", Body=FILE1_CONTENT.encode("utf-8"))
        yld.put_object(Key="repo/path/file2", Body=FILE2_CONTENT.encode("utf-8"))
        yld.put_object(Key="repo/path/file3", Body=FILE3_CONTENT.encode("utf-8"))
        yld.put_object(Key="repo/path/other_file", Body=OTHER_FILE_CONTENT.encode("utf-8"))
        yld.put_object(Key="repo/path/_control_/test_step.complete", Body=b"")
        yield yld


@pytest.fixture(scope="module")
def different_bucket():
    with moto.mock_s3():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=DIFFERENT_BUCKET)
        yld = boto3.resource("s3", region_name="us-east-1").Bucket(DIFFERENT_BUCKET)
        yld.put_object(Key="different/path/different_file", Body=DIFFERENT_FILE_CONTENT.encode("utf-8"))
        yield yld


@pytest.mark.parametrize("path, expect", [
    ("st*r", True),
    ("/question/mark?", True),
    ("/character/[set]", True),
    ("/not/a/glob", False),
])
def test_is_glob(path, expect):
    result = _is_glob(path)
    assert result == expect


@pytest.mark.parametrize("key, expect", [
    ("repo/path/file1", True),
    ("repo/path/file99", False),
])
def test_s3_file_exists(key, expect, repo_bucket):
    result = _s3_file_exists(key, TEST_BUCKET)
    assert result == expect


@pytest.mark.parametrize("glob, expect", [
    ("file*", ["file1", "file2", "file3"]),
    ("file?", ["file1", "file2", "file3"]),
    ("file[12]", ["file1", "file2"]),
    ("*file*", ["file1", "file2", "file3", "other_file"]),
    ("nothing*", []),
])
def test_expand_s3_glob(repo_bucket, glob, expect):
    ext_glob = f"s3://{TEST_BUCKET}/repo/path/{glob}"
    result = sorted(list(_expand_s3_glob(ext_glob)))
    ext_expect = [f"s3://{TEST_BUCKET}/repo/path/{x}" for x in expect]
    assert result == ext_expect


def test_inputerator(repo_bucket):
    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=DIFFERENT_BUCKET)
    different_bucket = boto3.resource("s3", region_name="us-east-1").Bucket(DIFFERENT_BUCKET)
    different_bucket.put_object(Key="different/path/different_file", Body=DIFFERENT_FILE_CONTENT.encode("utf-8"))

    paths = [
        "s3://test-bucket/repo/path/file*",
        "s3://different-bucket/path/different_file",
    ]
    result = sorted(list(_inputerator(paths)))
    expect = sorted([
        "s3://test-bucket/repo/path/file1",
        "s3://test-bucket/repo/path/file2",
        "s3://test-bucket/repo/path/file3",
        "s3://different-bucket/path/different_file",
    ])
    assert result == expect


@pytest.mark.parametrize("optional", [True, False])
def test_download_this(optional, tmp_path, repo_bucket):
    os.chdir(tmp_path)
    _download_this("s3://test-bucket/repo/path/file1", optional)
    expected_file = tmp_path / "file1"
    assert os.path.isfile(expected_file)
    with expected_file.open() as fp:
        line = fp.readline()
        assert line == FILE1_CONTENT


def test_download_this_missing_required_file(tmp_path, repo_bucket):
    target = "s3://test-bucket/repo/path/file99"
    os.chdir(tmp_path)
    with pytest.raises(RuntimeError, match=f"download failed: {target}"):
        _download_this(target, False)


def test_download_this_missing_optional_file(tmp_path, repo_bucket, caplog):
    target = "s3://test-bucket/repo/path/file99"
    os.chdir(tmp_path)
    _download_this(target, True)
    unexpected_file = tmp_path / "file99"
    assert "optional file not found" in caplog.text
    assert os.path.exists(unexpected_file) is False


def test_outputerator(tmp_path, caplog):
    filenames = "output1 output2 output3 other_thing ignore_me".split()
    for filename in filenames:
        file = tmp_path / filename
        file.open("w").close()
    request = ["output*", "other_thing", "non_thing*"]

    os.chdir(tmp_path)
    result = sorted(list(_outputerator(request)))
    expect = sorted("output1 output2 output3 other_thing".split())
    assert result == expect

    assert caplog.messages[0] == "no file matching 'non_thing*' found in workspace"


def test_upload_that(monkeypatch, tmp_path, repo_bucket):
    monkeypatch.setenv("BC_EXECUTION_ID", "ELVISLIVES")

    target_file = tmp_path / "output1"
    with target_file.open("w") as fp:
        print("target file", file=fp)

    _upload_that(str(target_file.absolute()), TEST_BUCKET, "repo/path")

    chek = repo_bucket.Object("repo/path/output1").get()

    expected_metadata = {"execution_id": "ELVISLIVES"}
    assert chek["Metadata"] == expected_metadata

    with closing(chek["Body"]) as fp:
        line = next(fp)
        assert line == "target file\n".encode("utf-8")


def test_upload_that_missing_file(tmp_path, caplog, repo_bucket):
    target_file = tmp_path / "missing"

    _upload_that(str(target_file.absolute()), TEST_BUCKET, "repo/path")
    assert f"{target_file} not found; skipping upload" in caplog.text


def test_upload_that_fail(tmp_path, repo_bucket):
    target_file = tmp_path / "outputx"

    with target_file.open("w") as fp:
        print("target file", file=fp)

    with pytest.raises(RuntimeError, match="upload failed: outputx -> s3://unbucket/repo/path/outputx"):
        _upload_that(str(target_file.absolute()), "unbucket", "repo/path")


def test_repository(monkeypatch):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    path = "s3://test-bucket/repo/path"
    repo = Repository(path)
    assert repo.full_path == path
    assert repo.bucket == "test-bucket"
    assert repo.prefix == "repo/path"
    assert repo.run_status_obj == "repo/path/_control_/test_step.complete"


def test_read_job_data(monkeypatch, repo_bucket):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo = Repository(f"s3://{TEST_BUCKET}/repo/path")
    job_data = repo.read_job_data()
    assert job_data == JOB_DATA


@pytest.mark.parametrize("name, expect", [
    ("file1", f"s3://{TEST_BUCKET}/repo/path/file1"),
    ("s3://some/path/to/a/file", "s3://some/path/to/a/file"),
])
def test_add_s3_path(monkeypatch, name, expect):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo = Repository(f"s3://{TEST_BUCKET}/repo/path")
    result = repo.add_s3_path(name)
    assert result == expect


@pytest.mark.parametrize("files, expect", [
    (["file1", "file2", "file3"], True),
    (["file1", "file99", "file3"], False),
    (["file1", "file*", "file3"], False),
    ([], True),
])
def test_files_exist(monkeypatch, repo_bucket, files, expect):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo = Repository(f"s3://{TEST_BUCKET}/repo/path")
    result = repo.files_exist(files)
    assert result == expect


@pytest.mark.parametrize("optional", [True, False])
def test_download_inputs(optional, monkeypatch, tmp_path, repo_bucket):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo = Repository(f"s3://{TEST_BUCKET}/repo/path")

    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=DIFFERENT_BUCKET)
    different_bucket = boto3.resource("s3", region_name="us-east-1").Bucket(DIFFERENT_BUCKET)
    different_bucket.put_object(Key="different/path/different_file", Body=DIFFERENT_FILE_CONTENT.encode("utf-8"))

    file_spec = {
        "files": "file*",
        "other_file": "other_file",
        "different_file": f"s3://{DIFFERENT_BUCKET}/different/path/different_file"
    }

    os.chdir(tmp_path)
    result = repo.download_inputs(file_spec, optional)
    expect = {
        "files": "file*",
        "other_file": "other_file",
        "different_file": "different_file"
    }

    for filename in "file1 file2 file3 other_file different_file".split():
        chek = tmp_path / filename
        assert chek.exists()

    assert result == expect


def test_download_inputs_missing_required_file(monkeypatch, tmp_path, repo_bucket):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo = Repository(f"s3://{TEST_BUCKET}/repo/path")

    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=DIFFERENT_BUCKET)
    different_bucket = boto3.resource("s3", region_name="us-east-1").Bucket(DIFFERENT_BUCKET)
    different_bucket.put_object(Key="different/path/different_file", Body=DIFFERENT_FILE_CONTENT.encode("utf-8"))

    file_spec = {
        "files": "file*",
        "other_file": "other_file",
        "different_file": f"s3://{DIFFERENT_BUCKET}/different/path/different_file",
        "missing_file": f"s3://{DIFFERENT_BUCKET}/missing_path/missing_file"
    }

    os.chdir(tmp_path)
    with pytest.raises(RuntimeError, match=f"download failed: {file_spec['missing_file']}"):
        _ = repo.download_inputs(file_spec, False)


def test_download_inputs_missing_optional_file(monkeypatch, tmp_path, caplog, repo_bucket):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo = Repository(f"s3://{TEST_BUCKET}/repo/path")

    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=DIFFERENT_BUCKET)
    different_bucket = boto3.resource("s3", region_name="us-east-1").Bucket(DIFFERENT_BUCKET)
    different_bucket.put_object(Key="different/path/different_file", Body=DIFFERENT_FILE_CONTENT.encode("utf-8"))

    file_spec = {
        "files": "file*",
        "other_file": "other_file",
        "different_file": f"s3://{DIFFERENT_BUCKET}/different/path/different_file",
        "missing_file": f"s3://{DIFFERENT_BUCKET}/missing_path/missing_file"
    }

    os.chdir(tmp_path)
    result = repo.download_inputs(file_spec, True)
    expect = {
        "files": "file*",
        "other_file": "other_file",
        "different_file": "different_file",
        "missing_file": "missing_file",
    }

    assert result == expect
    assert f"optional file not found: {file_spec['missing_file']}; skipping" in caplog.text
    assert os.path.exists(tmp_path / "file1")
    assert os.path.exists(tmp_path / "file2")
    assert os.path.exists(tmp_path / "file3")
    assert os.path.exists(tmp_path / "other_file")
    assert os.path.exists(tmp_path / "different_file")
    assert os.path.exists(tmp_path / "missing_file") is False


@pytest.mark.parametrize("optional", [True, False])
def test_download_inputs_empty_inputs(optional, monkeypatch, repo_bucket):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo = Repository(f"s3://{TEST_BUCKET}/repo/path")

    file_spec = {}

    result = repo.download_inputs(file_spec, optional)
    assert len(result) == 0


def test_upload_outputs(monkeypatch, tmp_path, repo_bucket):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo = Repository(f"s3://{TEST_BUCKET}/repo_two/path")

    for output_filename in "output1 output2 output3 other_output".split():
        file = tmp_path / output_filename
        with file.open("w") as fp:
            print(output_filename, file=fp)

    file_spec = {
        "outputs": "output*",
        "other_output": "other_output",
        "missing_file": "missing_file",
    }

    os.chdir(tmp_path)
    repo.upload_outputs(file_spec)

    repo_objects = boto3.client("s3", region_name="us-east-1").list_objects_v2(Bucket=TEST_BUCKET, Prefix="repo_two/path")
    repo_contents = sorted(jmespath.search("Contents[].Key", repo_objects))
    expect = sorted([
        "repo_two/path/output1",
        "repo_two/path/output2",
        "repo_two/path/output3",
        "repo_two/path/other_output",
    ])

    assert repo_contents == expect


def test_upload_outputs_fail(monkeypatch, tmp_path, repo_bucket):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo = Repository(f"s3://unbucket/repo_x/path")

    for output_filename in "output1 output2".split():
        file = tmp_path / output_filename
        with file.open("w") as fp:
            print(output_filename, file=fp)

    file_spec = {
        "outputs": "output*",
    }

    os.chdir(tmp_path)
    with pytest.raises(RuntimeError, match=f"upload failed:"):
        repo.upload_outputs(file_spec)


def test_upload_outputs_empty_outputs(monkeypatch, repo_bucket):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo = Repository(f"s3://{TEST_BUCKET}/repo/path")

    file_spec = {}

    repo.upload_outputs(file_spec)


@pytest.mark.parametrize("step_name, expect", [
    ("test_step", True),
    ("non_step", False),
])
def test_check_for_previous_run(monkeypatch, repo_bucket, step_name, expect):
    monkeypatch.setenv("BC_STEP_NAME", step_name)
    repo = Repository(f"s3://{TEST_BUCKET}/repo/path")
    result = repo.check_for_previous_run()
    assert result == expect


def test_clear_run_status(monkeypatch, repo_bucket):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo = Repository(f"s3://{TEST_BUCKET}/repo/path")
    assert repo.check_for_previous_run() is True
    repo.clear_run_status()
    assert repo.check_for_previous_run() is False


def test_put_run_status(monkeypatch, repo_bucket):
    monkeypatch.setenv("BC_STEP_NAME", "test_step_two")
    repo = Repository(f"s3://{TEST_BUCKET}/repo/path")
    assert repo.check_for_previous_run() is False
    repo.put_run_status()
    assert repo.check_for_previous_run() is True
