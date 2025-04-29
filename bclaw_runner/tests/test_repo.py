from contextlib import closing
import json
import os

import boto3
import jmespath
import moto
import pytest

from ..src.runner.repo import _is_glob, _expand_s3_glob, Repository, SkipExecution

TEST_BUCKET = "test-bucket"
JOB_DATA = {"job": "data"}
FILE1_CONTENT = "file one"
FILE2_CONTENT = "file two"
FILE3_CONTENT = "file three"
OTHER_FILE_CONTENT = "other file"

DIFFERENT_BUCKET = "different-bucket"
DIFFERENT_FILE_CONTENT = "different file"
NO_FOLDER_FILE1_CONTENT = "no folder file 1"
NO_FOLDER_FILE2_CONTENT = "no folder file 2"


@pytest.fixture(scope="module")
def mock_buckets():
    with moto.mock_aws():
        s3 = boto3.resource("s3", region_name="us-east-1")

        tb = s3.Bucket(TEST_BUCKET)
        tb.create()
        tb.put_object(Key="repo/path/_JOB_DATA_", Body=json.dumps(JOB_DATA).encode("utf-8"))
        tb.put_object(Key="repo/path/file1", Body=FILE1_CONTENT.encode("utf-8"))
        tb.put_object(Key="repo/path/file2", Body=FILE2_CONTENT.encode("utf-8"))
        tb.put_object(Key="repo/path/file3", Body=FILE3_CONTENT.encode("utf-8"))
        tb.put_object(Key="repo/path/other_file", Body=OTHER_FILE_CONTENT.encode("utf-8"))
        tb.put_object(Key="repo/path/_control_/test_step.complete", Body=b"")

        db = s3.Bucket(DIFFERENT_BUCKET)
        db.create()
        db.put_object(Key="different/path/different_file", Body=DIFFERENT_FILE_CONTENT.encode("utf-8"))
        db.put_object(Key="no_folder_file1", Body=NO_FOLDER_FILE1_CONTENT.encode("utf-8"))
        db.put_object(Key="no_folder_file2", Body=NO_FOLDER_FILE2_CONTENT.encode("utf-8"))

        yield tb, db


@pytest.mark.parametrize("path, expect", [
    ("st*r", True),
    ("/question/mark?", True),
    ("/character/[set]", True),
    ("/not/a/glob", False),
])
def test_is_glob(path, expect):
    result = _is_glob(path)
    assert result == expect


@pytest.mark.parametrize("glob, expect", [
    ("file*", ["file1", "file2", "file3"]),
    ("file?", ["file1", "file2", "file3"]),
    ("file[12]", ["file1", "file2"]),
    ("*file*", ["file1", "file2", "file3", "other_file"]),
    ("nothing*", []),
    ("file1", ["file1"]),
])
def test_expand_s3_glob(mock_buckets, glob, expect):
    ext_glob = f"s3://{TEST_BUCKET}/repo/path/{glob}"
    result = sorted(list(_expand_s3_glob(ext_glob)))
    ext_expect = [f"s3://{TEST_BUCKET}/repo/path/{x}" for x in expect]
    assert result == ext_expect


@pytest.mark.parametrize("glob, expect", [
    ("no_folder_file*", ["no_folder_file1", "no_folder_file2"]),
    ("no_folder_file1", ["no_folder_file1"]),
    ("*", ["no_folder_file1", "no_folder_file2", "different/path/different_file"]),
    ("[lmnop]*", ["no_folder_file1", "no_folder_file2"]),
    ("?iffer*", ["different/path/different_file"]),
    ("qwerty*", []),
])
def test_expand_s3_glob_no_folder(mock_buckets, glob, expect):
    ext_glob = f"s3://{DIFFERENT_BUCKET}/{glob}"
    result = sorted(list(_expand_s3_glob(ext_glob)))
    ext_expect = sorted([f"s3://{DIFFERENT_BUCKET}/{x}" for x in expect])
    assert result == ext_expect


def test_repository(monkeypatch):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo_uri = "s3://test-bucket/repo/path"
    repo = Repository(repo_uri)
    assert repo.s3_uri == repo_uri
    assert repo.bucket == "test-bucket"
    assert repo.prefix == "repo/path"
    assert repo.run_status_obj == "_control_/test_step.complete"


def test_to_uri(monkeypatch):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo_uri = "s3://test-bucket/repo/path"
    repo = Repository(repo_uri)
    filename = "path/to/file.txt"
    result = repo.to_uri(filename)
    assert result == f"{repo_uri}/{filename}"


def test_qualify(monkeypatch):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo_uri = "s3://test-bucket/repo/path"
    repo = Repository(repo_uri)
    filename = "path/to/file.txt"
    result = repo.qualify(filename)
    assert result == f"repo/path/{filename}"


def test_read_job_data(monkeypatch, mock_buckets):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo = Repository(f"s3://{TEST_BUCKET}/repo/path")
    job_data = repo.read_job_data()
    assert job_data == JOB_DATA


@pytest.mark.parametrize("key, expect", [
    ("repo/path/file1", True),
    ("repo/path/file99", False),
])
def test_s3_file_exists(monkeypatch, key, expect, mock_buckets):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo = Repository(f"s3://{TEST_BUCKET}/repo/path")
    result = repo._s3_file_exists(key)
    assert result == expect


@pytest.mark.parametrize("files, expect_skip", [
    (["file1", "file2", "subdir/file3"], True),
    (["file1", "file99", "subdir/file3"], False),
    (["file1", "file*", "subdir/file3"], False),
    ([], True),
])
def test_check_files_exist(monkeypatch, mock_buckets, files, expect_skip):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo = Repository(f"s3://{TEST_BUCKET}/repo/path")
    if expect_skip:
        with pytest.raises(SkipExecution):
            repo.check_files_exist(files)
    else:
        repo.check_files_exist(files)


def test_inputerator(monkeypatch, mock_buckets):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo_uri = f"s3://{TEST_BUCKET}/repo/path"
    repo = Repository(repo_uri)

    spec = {
        "files": "s3://test-bucket/repo/path/file*",
        "different_file": "s3://different-bucket/different/path/different_file",
    }
    result = sorted(list(repo._inputerator(spec)))
    expect = sorted([
        "s3://test-bucket/repo/path/file1",
        "s3://test-bucket/repo/path/file2",
        "s3://test-bucket/repo/path/file3",
        "s3://different-bucket/different/path/different_file",
    ])
    assert result == expect


@pytest.mark.parametrize("spec, expected_files", [
    ({"f": "file*"}, ["file1", "file2", "file3"]),
    ({"f?": "file?"}, ["file1", "file2", "file3"]),
    ({"f": "file[12]"}, ["file1", "file2"]),
    ({"f?": "*file*"}, ["file1", "file2", "file3", "other_file"]),
    ({"f?": "nothing*"}, []),
])
def test_inputerator_glob_expansion(monkeypatch, mock_buckets, spec, expected_files):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo_uri = f"s3://{TEST_BUCKET}/repo/path"
    repo = Repository(repo_uri)

    expect = [os.path.join(repo_uri, f) for f in expected_files]
    result = sorted(list(repo._inputerator(spec)))
    assert result == expect


@pytest.mark.parametrize("spec", [
    {"f": "missing"},
    {"f": "missing*"},
])
def test_inputerator_fail(monkeypatch, mock_buckets, spec):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo_uri = f"s3://{TEST_BUCKET}/repo/path"
    repo = Repository(repo_uri)

    with pytest.raises(FileNotFoundError):
        _ = list(repo._inputerator(spec))


def test_download_this(monkeypatch, mock_buckets, tmp_path):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo_uri = f"s3://{TEST_BUCKET}/repo/path"
    repo = Repository(repo_uri)

    os.chdir(tmp_path)
    repo._download_this("s3://test-bucket/repo/path/file1")
    expected_file = tmp_path / "file1"
    assert os.path.isfile(expected_file)
    with expected_file.open() as fp:
        line = fp.readline()
        assert line == FILE1_CONTENT


def test_download_this_missing_file(monkeypatch, mock_buckets, tmp_path):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo_uri = f"s3://{TEST_BUCKET}/repo/path"
    repo = Repository(repo_uri)

    target = "s3://test-bucket/repo/path/file99"
    os.chdir(tmp_path)
    with pytest.raises(FileNotFoundError, match=target):
        repo._download_this(target)


def test_download_inputs(monkeypatch, tmp_path, mock_buckets):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo = Repository(f"s3://{TEST_BUCKET}/repo/path")

    file_spec = {
        "files": "file*",
        "other_file": "other_file",
        "different_file": f"s3://{DIFFERENT_BUCKET}/different/path/different_file"
    }

    os.chdir(tmp_path)
    result = repo.download_inputs(file_spec)
    expect = {
        "files": "file*",
        "other_file": "other_file",
        "different_file": "different_file"
    }

    for filename in "file1 file2 file3 other_file different_file".split():
        chek = tmp_path / filename
        assert chek.exists()

    assert result == expect


def test_download_inputs_missing_required_file(monkeypatch, tmp_path, mock_buckets):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo = Repository(f"s3://{TEST_BUCKET}/repo/path")

    file_spec = {
        "files": "file*",
        "other_file": "other_file",
        "different_file": f"s3://{DIFFERENT_BUCKET}/different/path/different_file",
        "missing_file": f"s3://{DIFFERENT_BUCKET}/missing_path/missing_file"
    }

    os.chdir(tmp_path)
    with pytest.raises(FileNotFoundError):
        _ = repo.download_inputs(file_spec)


def test_download_inputs_missing_optional_file(monkeypatch, tmp_path, caplog, mock_buckets):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo = Repository(f"s3://{TEST_BUCKET}/repo/path")

    file_spec = {
        "files": "file*",
        "other_file": "other_file",
        "different_file": f"s3://{DIFFERENT_BUCKET}/different/path/different_file",
        "missing_file?": f"s3://{DIFFERENT_BUCKET}/missing_path/missing_file"
    }

    os.chdir(tmp_path)
    result = repo.download_inputs(file_spec)
    expect = {
        "files": "file*",
        "other_file": "other_file",
        "different_file": "different_file",
        "missing_file": "missing_file",
    }

    assert result == expect
    assert f"optional file not found: {file_spec['missing_file?']}; skipping" in caplog.text
    assert os.path.exists(tmp_path / "file1")
    assert os.path.exists(tmp_path / "file2")
    assert os.path.exists(tmp_path / "file3")
    assert os.path.exists(tmp_path / "other_file")
    assert os.path.exists(tmp_path / "different_file")
    assert os.path.exists(tmp_path / "missing_file") is False


def test_download_inputs_empty_inputs(monkeypatch, mock_buckets):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo = Repository(f"s3://{TEST_BUCKET}/repo/path")

    file_spec = {}

    result = repo.download_inputs(file_spec)
    assert len(result) == 0


# todo: delete
# def test_outputerator(monkeypatch, tmp_path, caplog):
#     monkeypatch.setenv("BC_STEP_NAME", "test_step")
#     repo = Repository(f"s3://{TEST_BUCKET}/repo/path")
#
#     filenames = "output1 output2 output3 other_thing ignore_me".split()
#     for filename in filenames:
#         file = tmp_path / filename
#         file.open("w").close()
#     request = ["output*", "other_thing", "non_thing*"]
#
#     os.chdir(tmp_path)
#     result = sorted(list(repo._outputerator(request)))
#     expect = sorted("output1 output2 output3 other_thing".split())
#     assert result == expect
#
#     assert caplog.messages[0] == "no file matching 'non_thing*' found in workspace"


def test_outputerator(monkeypatch, tmp_path, caplog):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo = Repository(f"s3://{TEST_BUCKET}/repo/path")

    filenames = "output1 output2 output3 other_thing ignore_me".split()
    for filename in filenames:
        file = tmp_path / filename
        file.open("w").close()

    output_spec = {
        "outputs": {
            "name": "output*",
            "s3_tags": {"tag1": "value1"},
        },
        "other_thing!": {
            "name": "other_thing",
            "s3_tags": {"tag2": "value2"},
        },
        "non_thing": {
            "name": "non_thing*",
            "s3_tags": {"tag3": "value3"},
        },
    }

    expect = [
        ("other_thing!", {"name": "other_thing", "s3_tags": {"tag2": "value2"}}),
        ("outputs", {"name": "output1", "s3_tags": {"tag1": "value1"}}),
        ("outputs", {"name": "output2", "s3_tags": {"tag1": "value1"}}),
        ("outputs", {"name": "output3", "s3_tags": {"tag1": "value1"}}),
    ]

    os.chdir(tmp_path)
    result = sorted(list(repo._outputerator(output_spec)), key=lambda x: x[1]["name"])
    assert result == expect
    assert caplog.messages[0] == "no file matching 'non_thing*' found in workspace"


# todo: delete
# def test_outputerator_recursive_glob(monkeypatch, tmp_path):
#     monkeypatch.setenv("BC_STEP_NAME", "test_step")
#     repo = Repository(f"s3://{TEST_BUCKET}/repo/path")
#
#     targets = [
#         tmp_path / "dir1" / "dir2" / "dir3" / "file1.txt",
#         tmp_path / "dir4" / "dir5" / "file2.txt"
#     ]
#     for target in targets:
#         target.parent.mkdir(exist_ok=True, parents=True)
#         target.write_text("foo")
#
#     os.chdir(tmp_path)
#     request = ["dir*/**/file*.txt"]
#     result = sorted(list(repo._outputerator(request)))
#     assert result[0] == "dir1/dir2/dir3/file1.txt"
#     assert result[1] == "dir4/dir5/file2.txt"


def test_outputerator_recursive_glob(monkeypatch, tmp_path):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo = Repository(f"s3://{TEST_BUCKET}/repo/path")

    targets = [
        tmp_path / "dir1" / "dir2" / "dir3" / "file1.txt",
        tmp_path / "dir4" / "dir5" / "file2.txt"
    ]
    for target in targets:
        target.parent.mkdir(exist_ok=True, parents=True)
        target.write_text("foo")

    out_spec = {
        "files": {
            "name": "dir*/**/file*.txt",
            "s3_tags": {"tag1": "value1"},
        }
    }

    expect = [
        ("files", {"name": "dir1/dir2/dir3/file1.txt", "s3_tags": {"tag1": "value1"}}),
        ("files", {"name": "dir4/dir5/file2.txt", "s3_tags": {"tag1": "value1"}}),
    ]

    os.chdir(tmp_path)
    result = sorted(repo._outputerator(out_spec), key=lambda x: x[1]["name"])
    assert result == expect


# todo: delete
# def test_upload_that(monkeypatch, tmp_path, mock_buckets):
#     monkeypatch.setenv("BC_EXECUTION_ID", "ELVISLIVES")
#     monkeypatch.setenv("BC_STEP_NAME", "test_step")
#     repo = Repository(f"s3://{TEST_BUCKET}/repo/path")
#
#     target_file = tmp_path / "output1"
#     with target_file.open("w") as fp:
#         print("target file", file=fp)
#
#     repo._upload_that(str(target_file.absolute()))
#
#     test_bucket, _ = mock_buckets
#     chek = test_bucket.Object("repo/path/output1").get()
#
#     expected_metadata = {"execution_id": "ELVISLIVES"}
#     assert chek["Metadata"] == expected_metadata
#
#     with closing(chek["Body"]) as fp:
#         line = next(fp)
#         assert line == "target file\n".encode("utf-8")


def test_upload_that(monkeypatch, tmp_path, mock_buckets):
    monkeypatch.setenv("BC_EXECUTION_ID", "ELVISLIVES")
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo = Repository(f"s3://{TEST_BUCKET}/repo/path")

    target_file = tmp_path / "output1"
    with target_file.open("w") as fp:
        print("target file", file=fp)

    global_tags = {"tag1": "valueX", "tag2": "valueY"}
    file_spec = {
        "name": str(target_file.absolute()),
        "s3_tags": {"tag1": "value1", "tag3": "value3"},
    }

    repo._upload_that("sym_name", file_spec, global_tags)

    test_bucket, _ = mock_buckets
    chek = test_bucket.Object("repo/path/output1").get()

    expected_metadata = {"execution_id": "ELVISLIVES"}
    assert chek["Metadata"] == expected_metadata

    with closing(chek["Body"]) as fp:
        line = next(fp)
        assert line == "target file\n".encode("utf-8")

    expected_tags = [
        {"Key": "tag1", "Value": "value1"},
        {"Key": "tag2", "Value": "valueY"},
        {"Key": "tag3", "Value": "value3"},
    ]
    resp = test_bucket.meta.client.get_object_tagging(Bucket=TEST_BUCKET, Key="repo/path/output1")
    tags = sorted(resp["TagSet"], key=lambda x: x["Key"])
    assert tags == expected_tags


# todo: delete
# def test_upload_that_missing_file(monkeypatch, tmp_path, caplog, mock_buckets):
#     monkeypatch.setenv("BC_STEP_NAME", "test_step")
#     repo = Repository(f"s3://{TEST_BUCKET}/repo/path")
#
#     target_file = tmp_path / "missing"
#
#     with pytest.raises(FileNotFoundError):
#         repo._upload_that(str(target_file.absolute()))


def test_upload_that_missing_file(monkeypatch, tmp_path, caplog, mock_buckets):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo = Repository(f"s3://{TEST_BUCKET}/repo/path")

    target_file = tmp_path / "missing"
    output_spec = {
        "name": str(target_file),
        "s3_tags": {}
    }
    global_tags = {}

    with pytest.raises(FileNotFoundError):
        repo._upload_that("sym_name", output_spec, global_tags)


# todo: delete
# def test_upload_that_fail(monkeypatch, tmp_path, mock_buckets):
#     monkeypatch.setenv("BC_EXECUTION_ID", "ELVISLIVES")
#     monkeypatch.setenv("BC_STEP_NAME", "test_step")
#     repo = Repository(f"s3://unbucket/repo/path")
#
#     target_file = tmp_path / "outputx"
#
#     with target_file.open("w") as fp:
#         print("target file", file=fp)
#
#     with pytest.raises(boto3.exceptions.S3UploadFailedError):
#         repo._upload_that(str(target_file.absolute()))


def test_upload_that_fail(monkeypatch, tmp_path, mock_buckets):
    monkeypatch.setenv("BC_EXECUTION_ID", "ELVISLIVES")
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo = Repository(f"s3://unbucket/repo/path")

    target_file = tmp_path / "outputx"

    with target_file.open("w") as fp:
        print("target file", file=fp)

    output_spec = {
        "name": str(target_file),
        "s3_tags": {}
    }
    global_tags = {}

    with pytest.raises(boto3.exceptions.S3UploadFailedError):
        repo._upload_that("sym_name", output_spec, global_tags)


# todo: delete
# def test_upload_outputs(monkeypatch, tmp_path, mock_buckets):
#     monkeypatch.setenv("BC_STEP_NAME", "test_step")
#     repo = Repository(f"s3://{TEST_BUCKET}/repo_two/path")
#
#     for output_filename in "output1 output2 output3 other_output".split():
#         file = tmp_path / output_filename
#         with file.open("w") as fp:
#             print(output_filename, file=fp)
#
#     file_spec = {
#         "outputs": "output*",
#         "other_output": "other_output",
#         "missing_file": "missing_file",
#     }
#
#     os.chdir(tmp_path)
#     repo.upload_outputs(file_spec)
#
#     repo_objects = boto3.client("s3", region_name="us-east-1").list_objects_v2(Bucket=TEST_BUCKET, Prefix="repo_two/path")
#     repo_contents = sorted(jmespath.search("Contents[].Key", repo_objects))
#     expect = sorted([
#         "repo_two/path/output1",
#         "repo_two/path/output2",
#         "repo_two/path/output3",
#         "repo_two/path/other_output",
#     ])
#
#     assert repo_contents == expect


def test_upload_outputs(monkeypatch, tmp_path, mock_buckets):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo = Repository(f"s3://{TEST_BUCKET}/repo_two/path")

    for output_filename in "output1 output2 output3 other_output".split():
        file = tmp_path / output_filename
        with file.open("w") as fp:
            print(output_filename, file=fp)

    global_tags = {}
    output_spec = {
        "outputs": {
            "name": "output*",
            "s3_tags": {"tag1": "value1"},
        },
        "other_thing!": {
            "name": "other_output",
            "s3_tags": {"tag2": "value2"},
        },
        "non_thing": {
            "name": "missing_file",
            "s3_tags": {},
        },
    }

    os.chdir(tmp_path)
    repo.upload_outputs(output_spec, global_tags)

    repo_objects = boto3.client("s3", region_name="us-east-1").list_objects_v2(Bucket=TEST_BUCKET, Prefix="repo_two/path")
    repo_contents = sorted(jmespath.search("Contents[].Key", repo_objects))
    expect = sorted([
        "repo_two/path/output1",
        "repo_two/path/output2",
        "repo_two/path/output3",
        "repo_two/path/other_output",
    ])

    assert repo_contents == expect


# todo: delete
# def test_upload_outputs_fail(monkeypatch, tmp_path, mock_buckets):
#     monkeypatch.setenv("BC_STEP_NAME", "test_step")
#     repo = Repository(f"s3://unbucket/repo_x/path")
#
#     for output_filename in "output1 output2".split():
#         file = tmp_path / output_filename
#         with file.open("w") as fp:
#             print(output_filename, file=fp)
#
#     file_spec = {
#         "outputs": "output*",
#     }
#
#     os.chdir(tmp_path)
#     with pytest.raises(boto3.exceptions.S3UploadFailedError):
#         repo.upload_outputs(file_spec)


def test_upload_outputs_fail(monkeypatch, tmp_path, mock_buckets):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo = Repository(f"s3://unbucket/repo_x/path")

    for output_filename in "output1 output2".split():
        file = tmp_path / output_filename
        with file.open("w") as fp:
            print(output_filename, file=fp)

    global_tags = {}
    output_spec = {
        "outputs": {
            "name": "output*",
            "s3_tags": {"tag1": "value1"}
        }
    }

    os.chdir(tmp_path)
    with pytest.raises(boto3.exceptions.S3UploadFailedError):
        repo.upload_outputs(output_spec, global_tags)


# todo: delete
# def test_upload_outputs_empty_outputs(monkeypatch, mock_buckets):
#     monkeypatch.setenv("BC_STEP_NAME", "test_step")
#     repo = Repository(f"s3://{TEST_BUCKET}/repo/path")
#
#     file_spec = {}
#
#     repo.upload_outputs(file_spec)


def test_upload_outputs_empty_outputs(monkeypatch, mock_buckets):
    monkeypatch.setenv("BC_STEP_NAME", "test_step")
    repo = Repository(f"s3://{TEST_BUCKET}/repo/path")

    output_spec = {}
    global_tags = {"tag1": "value1"}

    repo.upload_outputs(output_spec, global_tags)


@pytest.mark.parametrize("step_name, expect_skip", [
    ("test_step", True),
    ("non_step", False),
])
def test_check_for_previous_run(monkeypatch, mock_buckets, step_name, expect_skip):
    monkeypatch.setenv("BC_STEP_NAME", step_name)
    repo = Repository(f"s3://{TEST_BUCKET}/repo/path")
    if expect_skip:
        with pytest.raises(SkipExecution):
            repo.check_for_previous_run()
    else:
        repo.check_for_previous_run()


def failing_thing(*args, **kwargs):
    raise RuntimeError("failed")


def test_check_for_previous_run_fail(monkeypatch, mock_buckets, caplog):
    monkeypatch.setenv("BC_STEP_NAME", "nothing")
    repo = Repository("s3://non_bucket/repo/path")
    monkeypatch.setattr(repo, "_s3_file_exists", failing_thing)
    repo.check_for_previous_run()
    assert "unable to query previous run status" in caplog.text


@pytest.mark.parametrize("step_name", [
    "test_step",
    "un_step",
])
def test_clear_run_status(monkeypatch, mock_buckets, step_name):
    monkeypatch.setenv("BC_STEP_NAME", step_name)
    repo = Repository(f"s3://{TEST_BUCKET}/repo/path")
    repo.clear_run_status()
    repo.check_for_previous_run()


def test_clear_run_status_fail(monkeypatch, mock_buckets, caplog):
    monkeypatch.setenv("BC_STEP_NAME", "nothing")
    repo = Repository("s3://non_bucket/repo/path")
    monkeypatch.setattr(repo, "qualify", failing_thing)
    repo.clear_run_status()
    assert "unable to clear previous run status" in caplog.text


def test_put_run_status(monkeypatch, mock_buckets):
    monkeypatch.setenv("BC_STEP_NAME", "test_step_two")
    repo = Repository(f"s3://{TEST_BUCKET}/repo/path")
    repo.check_for_previous_run()
    repo.put_run_status()
    with pytest.raises(SkipExecution):
        repo.check_for_previous_run()


def test_put_run_status_fail(monkeypatch, mock_buckets, caplog):
    monkeypatch.setenv("BC_STEP_NAME", "nothing")
    repo = Repository("s3://non_bucket/repo/path")
    monkeypatch.setattr(repo, "qualify", failing_thing)
    repo.put_run_status()
    assert "failed to upload run status" in caplog.text


