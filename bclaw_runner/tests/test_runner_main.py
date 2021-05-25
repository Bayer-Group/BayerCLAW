from contextlib import closing
import json
import os
import re
import textwrap
import time

import boto3
import moto
import pytest

from ..src.runner.runner_main import split_inputs, main, cli


TEST_BUCKET = "test-bucket"
JOB_DATA = {
    "job": {
        "key1": 1,
        "key2": 2,
    },
    "parent": {
        "value": "parent_value"
    },
    "scatter": {
        "value": "scatter_value"
    }
}


@pytest.fixture(scope="module")
def mock_bucket():
    with moto.mock_s3():
        yld = boto3.resource("s3").Bucket(TEST_BUCKET)
        yld.create()
        yld.put_object(Key="repo/path/_JOB_DATA_", Body=json.dumps(JOB_DATA).encode("utf-8"))
        yld.put_object(Key="repo/path/file1", Body=b"file one")
        yld.put_object(Key="repo/path/file2", Body=b"file two")
        yld.put_object(Key="repo/path/file3", Body=b"file three")
        yld.put_object(Key="repo/path/_control_/step0.complete", Body=b"")
        yld.put_object(Key="references/reference_file", Body=b"reference")
        yield yld


def mock_get_config(cfg: dict):
    def _ret():
        return cfg
    return _ret


req_inputs = {
    "req1": "file1.txt",
    "req2": "file2.txt",
}

opt_inputs = {
    "opt1?": "file3.txt",
    "opt2?": "file4.txt",
}


@pytest.mark.parametrize("all_inputs, expected_req, expected_opt", [
    ({**req_inputs, **opt_inputs}, req_inputs, opt_inputs),
    (req_inputs, req_inputs, {}),
    (opt_inputs, {}, opt_inputs),
    ({}, {}, {}),
])
def test_split_inputs(all_inputs, expected_req, expected_opt):
    req_result, opt_result = split_inputs(all_inputs)
    assert req_result == expected_req
    assert len(opt_result) == len(expected_opt)
    for k, v in opt_result.items():
        orig_key = k + "?"
        assert orig_key in expected_opt
        assert v == expected_opt[orig_key]


def test_main(monkeypatch, tmp_path, mock_bucket, read_config):
    monkeypatch.setenv("BC_STEP_NAME", "step1")
    monkeypatch.setenv("BC_SCRATCH_PATH", str(tmp_path))
    monkeypatch.setattr("bclaw_runner.src.runner.runner_main.get_config", mock_get_config(read_config))

    references = {
        "ref1": f"s3://{TEST_BUCKET}/references/reference_file",
    }

    params = {
        "param1": "3"
    }

    inputs = {
        "input1": "file${job.key1}",
        "input2": "file${job.key2}",
        "input3?": "file${param1}",
        "input4?": "file99"
    }

    outputs = {
        "output1": "outfile${job.key1}",
        "output2": "outfile${job.key2}",
        "output3": "outfile${param1}"
    }

    commands = [
        "ls -1 > ${output1}",
        "env | grep '^BC_' > ${output2}",
        "echo ${job.key1} > ${output3}",
        "echo ${job.key2} >> ${output3}",
        "echo ${parent.value} >> ${output3}",
        "echo ${scatter.value} >> ${output3}",
        "echo ${param1} >> ${output3}"
        "echo ${input4} >> ${output3}"
    ]

    orig_bucket_contents = {o.key for o in mock_bucket.objects.all()}

    response = main(commands=commands,
                    references=references,
                    inputs=inputs,
                    outputs=outputs,
                    params=params,
                    repo_path=f"s3://{TEST_BUCKET}/repo/path",
                    skip="true")
    assert response == 0

    curr_bucket_contents = {o.key for o in mock_bucket.objects.all()}
    nu_objects = curr_bucket_contents - orig_bucket_contents
    expect_nu_objects = {
        "repo/path/outfile1",
        "repo/path/outfile2",
        "repo/path/outfile3",
        "repo/path/_control_/step1.complete",
    }
    assert nu_objects == expect_nu_objects

    expect_outfile1_contents = textwrap.dedent(r"""
        _commands\.sh
        file1
        file2
        file3
        job_data_\w+\.json
        outfile1
        reference_file
    """)
    outfile1 = mock_bucket.Object("repo/path/outfile1").get()
    with closing(outfile1["Body"]) as fp:
        outfile1_contents = next(fp).decode("utf-8").split()
        for name, pattern in zip(outfile1_contents, expect_outfile1_contents.split()):
            assert re.fullmatch(pattern, name)

    expect_outfile2_contents = textwrap.dedent(fr"""
        BC_JOB_DATA_FILE={tmp_path}/tmp\w+/job_data_\w+\.json
        BC_SCRATCH_PATH={tmp_path}
        BC_STEP_NAME=step1
        BC_WORKSPACE={tmp_path}/\w+
    """)
    outfile2 = mock_bucket.Object("repo/path/outfile2").get()
    with closing(outfile2["Body"]) as fp:
        outfile2_contents = sorted(next(fp).decode("utf-8").split())
        for value, pattern in zip(outfile2_contents, expect_outfile2_contents.split()):
            assert re.fullmatch(pattern, value)

    expect_outfile3_contents = textwrap.dedent("""
    1
    2
    parent_value
    scatter_value
    3
    file99
    """)
    outfile3 = mock_bucket.Object("repo/path/outfile3").get()
    with closing(outfile3["Body"]) as fp:
        outfile3_contents = next(fp).decode("utf-8").split()
        for line, expect in zip(outfile3_contents, expect_outfile3_contents.split()):
            assert line == expect

    reference_file_etag = mock_bucket.Object("references/reference_file").e_tag.strip('"')
    expected_cached_file = f"{tmp_path}/{reference_file_etag}/reference_file"
    assert os.path.isfile(expected_cached_file)
    with open(expected_cached_file) as fp:
        contents = next(fp)
        assert contents == "reference"


def test_main_fail_before_commands(monkeypatch, tmp_path, mock_bucket, read_config):
    monkeypatch.setenv("BC_STEP_NAME", "step2")
    monkeypatch.setenv("BC_SCRATCH_PATH", str(tmp_path))
    monkeypatch.setattr("bclaw_runner.src.runner.runner_main.get_config", mock_get_config(read_config))

    references = {}

    inputs = {
        "input1": "file${job.key1}",
        "input2": "file${job.key2}",
        "input3": "this_file_does_not_exist",
    }

    outputs = {
        "output4": "outfile4"
    }

    commands = [
        "echo wut > ${output4}"
    ]

    orig_bucket_contents = {o.key for o in mock_bucket.objects.all()}

    response = main(commands=commands,
                    references=references,
                    inputs=inputs,
                    outputs=outputs,
                    params={},
                    repo_path=f"s3://{TEST_BUCKET}/repo/path",
                    skip="true")

    assert response == 255
    curr_bucket_contents = {o.key for o in mock_bucket.objects.all()}
    assert curr_bucket_contents == orig_bucket_contents


def test_main_fail_in_commands(monkeypatch, tmp_path, mock_bucket, read_config):
    monkeypatch.setenv("BC_STEP_NAME", "step3")
    monkeypatch.setenv("BC_SCRATCH_PATH", str(tmp_path))
    monkeypatch.setattr("bclaw_runner.src.runner.runner_main.get_config", mock_get_config(read_config))

    references = {}
    inputs = {}
    outputs = {
        "output5": "outfile5"
    }

    commands = [
        "echo wut > ${output5}",
        "false"
    ]

    orig_bucket_contents = {o.key for o in mock_bucket.objects.all()}

    response = main(commands=commands,
                    references=references,
                    inputs=inputs,
                    outputs=outputs,
                    params={},
                    repo_path=f"s3://{TEST_BUCKET}/repo/path",
                    skip="true")
    assert response != 0

    curr_bucket_contents = {o.key for o in mock_bucket.objects.all()}
    nu_objects = curr_bucket_contents - orig_bucket_contents
    assert nu_objects == {"repo/path/outfile5"}


def failing_uploader(*args, **kwargs):
    raise RuntimeError("miscellaneous error")


def test_main_fail_after_commands(monkeypatch, tmp_path, mock_bucket, read_config):
    monkeypatch.setenv("BC_STEP_NAME", "step4")
    monkeypatch.setenv("BC_SCRATCH_PATH", str(tmp_path))
    monkeypatch.setattr("bclaw_runner.src.runner.runner_main.get_config", mock_get_config(read_config))
    monkeypatch.setattr("bclaw_runner.src.runner.repo._upload_that", failing_uploader)

    references = {}
    inputs = {}
    outputs = {"output6": "outfile6"}
    commands = ["echo wut > ${output6}"]

    response = main(commands=commands,
                    references=references,
                    inputs=inputs,
                    outputs=outputs,
                    params={},
                    repo_path=f"s3://{TEST_BUCKET}/repo/path",
                    skip="true")
    assert response != 0
    curr_bucket_contents = {o.key for o in mock_bucket.objects.all()}
    assert "repo/path/_control_/step4.complete" not in curr_bucket_contents


@pytest.mark.parametrize("skip, expect", [
    ("rerun", 0),
    ("output", 0),
    ("none", 1)
])
def test_main_skip(monkeypatch, tmp_path, mock_bucket, skip, expect, read_config):
    monkeypatch.setenv("BC_STEP_NAME", "step0")
    monkeypatch.setenv("BC_SCRATCH_PATH", str(tmp_path))
    monkeypatch.setattr("bclaw_runner.src.runner.runner_main.get_config", mock_get_config(read_config))

    references = {}
    inputs = {}
    outputs = {}
    commands = ["false"]

    response = main(commands=commands,
                    references=references,
                    inputs=inputs,
                    outputs=outputs,
                    params={},
                    repo_path=f"s3://{TEST_BUCKET}/repo/path",
                    skip=skip)
    assert response == expect


def fake_main(*args):
    print("fake main running")
    time.sleep(1)
    return list(args)


def fake_termination_checker_impl(*_):
    print("fake termination checker running")


@moto.mock_logs
@pytest.mark.parametrize("argv, expect", [
    ("prog --cmd 2 --in 3 --out 4 --param 5 --ref 6 --repo 7 --skip 8",
     [2, 3, 4, 5, 6, "7", "8"]),
    ("prog --cmd 2 --in 3 --out 4 --ref 6 --repo 7 --skip 8",
     [2, 3, 4, {}, 6, "7", "8"])
])
def test_cli(capsys, requests_mock, monkeypatch, argv, expect):
    requests_mock.get("http://169.254.169.254/latest/meta-data/instance-life-cycle", text="spot")
    requests_mock.get("http://169.254.169.254/latest/meta-data/spot/instance-action", status_code=404)
    monkeypatch.setenv("BC_WORKFLOW_NAME", "testWorkflowName")
    monkeypatch.setenv("BC_STEP_NAME", "test:step:name")
    monkeypatch.setenv("BC_JOB_NAME", "test*job")
    monkeypatch.setenv("BC_S3_REQUEST_ID", "12345ELVISLIVES")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setattr("sys.argv", argv.split())
    monkeypatch.setattr("bclaw_runner.src.runner.runner_main.main", fake_main)
    monkeypatch.setattr("bclaw_runner.src.runner.termination._termination_checker_impl", fake_termination_checker_impl)

    response = cli()
    assert response == expect

    captured = capsys.readouterr()
    assert "fake main running" in captured.out
    assert "fake termination checker running" in captured.out
