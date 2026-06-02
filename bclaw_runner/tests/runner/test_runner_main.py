import os
import json

import pytest
from unittest import mock
from moto import mock_aws
import boto3
import tempfile
from pathlib import Path

import bclaw_runner.src.runner.runner_main as runner_main

# Helper to set required environment variables
def set_env_vars():
    os.environ["BC_LAUNCH_BUCKET"] = "test-bucket"
    os.environ["BC_LAUNCH_KEY"] = "test-key"
    os.environ["BC_LAUNCH_VERSION"] = "test-version"
    os.environ["BC_TASK_TOKEN"] = "test-token"
    os.environ["BC_VERSION"] = "1.0.0"

@pytest.fixture(autouse=True)
def env_setup(monkeypatch):
    set_env_vars()
    yield
    # Clean up
    for k in ["BC_LAUNCH_BUCKET", "BC_LAUNCH_KEY", "BC_LAUNCH_VERSION", "BC_TASK_TOKEN", "BC_VERSION"]:
        os.environ.pop(k, None)


def test_read_jobfile():
    with mock_aws():
        set_env_vars()
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket="test-bucket")
        job_data = {"foo": "bar"}
        s3.put_object(Bucket="test-bucket", Key="test-key", Body=json.dumps(job_data))
        # Moto does not support VersionId, so patch get_object
        with mock.patch.object(s3, "get_object", return_value={"Body": mock.Mock(read=lambda: json.dumps(job_data), __enter__=lambda s: s, __exit__=lambda s, exc_type, exc_val, exc_tb: None, readlines=lambda: [json.dumps(job_data)])}):
            with mock.patch("boto3.client", return_value=s3):
                result = runner_main.read_jobfile()
                assert result == job_data

@mock.patch("bclaw_runner.src.runner.runner_main.run_child_container")
@mock.patch("bclaw_runner.src.runner.runner_main.Workspace")
def test_command_runner_success(mock_workspace, mock_run_child):
    mock_run_child.return_value = 0
    mock_ws = mock.MagicMock()
    # Set runner_path and child_path to valid temp paths
    temp_dir = tempfile.gettempdir()
    mock_ws.runner_path = temp_dir
    mock_ws.child_path = Path(temp_dir)
    mock_workspace.return_value.__enter__.return_value = mock_ws
    commands = ["echo hello"]
    imports = ["file1.txt"]
    exports = ["file2.txt"]
    image_spec = {"Image": "test"}
    repo = "/tmp"
    shell = "sh"
    with mock.patch("bclaw_runner.src.runner.runner_main.read_jobfile", return_value={}):
        runner_main.command_runner(commands, imports, exports, image_spec, repo, shell)
    mock_run_child.assert_called_once()
    mock_ws.do_exports.assert_called_once()

@mock.patch("bclaw_runner.src.runner.runner_main.run_child_container")
@mock.patch("bclaw_runner.src.runner.runner_main.Workspace")
def test_command_runner_failure(mock_workspace, mock_run_child):
    mock_run_child.return_value = 1
    mock_ws = mock.MagicMock()
    temp_dir = tempfile.gettempdir()
    mock_ws.runner_path = temp_dir
    mock_ws.child_path = Path(temp_dir)
    mock_workspace.return_value.__enter__.return_value = mock_ws
    commands = ["echo fail"]
    imports = ["file1.txt"]
    exports = ["file2.txt"]
    image_spec = {"Image": "test"}
    repo = "/tmp"
    shell = "sh"
    with mock.patch("bclaw_runner.src.runner.runner_main.read_jobfile", return_value={}):
        with pytest.raises(runner_main.UserCommandsFailed):
            runner_main.command_runner(commands, imports, exports, image_spec, repo, shell)


def test_main_success():
    with mock_aws():
        set_env_vars()
        sfn = boto3.client("stepfunctions", region_name="us-east-1")
        # Patch send_task_success/failure
        with mock.patch("boto3.client", return_value=sfn):
            with mock.patch("bclaw_runner.src.runner.runner_main.command_runner") as mock_cmd:
                mock_cmd.return_value = None
                result = runner_main.main(["echo ok"], [], [], {}, "/tmp", "sh")
                assert result == 0
                mock_cmd.assert_called_once()

def test_main_user_commands_failed():
    with mock_aws():
        set_env_vars()
        sfn = boto3.client("stepfunctions", region_name="us-east-1")
        with mock.patch("boto3.client", return_value=sfn):
            with mock.patch("bclaw_runner.src.runner.runner_main.command_runner", side_effect=runner_main.UserCommandsFailed("fail", 123)):
                result = runner_main.main(["fail"], [], [], {}, "/tmp", "sh")
                assert result == 123

def test_main_exception():
    with mock_aws():
        set_env_vars()
        sfn = boto3.client("stepfunctions", region_name="us-east-1")
        with mock.patch("boto3.client", return_value=sfn):
            with mock.patch("bclaw_runner.src.runner.runner_main.command_runner", side_effect=Exception("boom")):
                result = runner_main.main(["fail"], [], [], {}, "/tmp", "sh")
                assert result == 199

