import os
import json

import pytest

from ..src.runner.workspace import workspace, write_job_data_file, run_commands


def test_workspace(monkeypatch, tmp_path):
    monkeypatch.setenv("BC_SCRATCH_PATH", str(tmp_path))
    orig_dir = os.getcwd()

    with workspace() as wrk:
        assert os.path.isdir(wrk)
        assert os.getcwd() == wrk
        assert os.path.dirname(wrk) == str(tmp_path)

    assert os.getcwd() == orig_dir
    assert not os.path.isdir(wrk)


def test_write_job_data_file(tmp_path):
    job_data = {
        "one": 1,
        "two": 2,
        "three": 3,
    }

    jdf = write_job_data_file(job_data, str(tmp_path))

    assert os.path.exists(jdf)
    assert os.path.dirname(jdf) == str(tmp_path)

    with open(jdf) as fp:
        jdf_contents = json.load(fp)
    assert jdf_contents == job_data


def test_run_commands(tmp_path, read_config):
    f = tmp_path / "test_success.out"

    commands = [
        f"echo 'one' > {str(f)}",
        "z='two'",
        f"echo $z >> {str(f)}"
    ]

    os.chdir(tmp_path)
    response = run_commands(commands, read_config, str(tmp_path), f"{tmp_path}/fake_job_data.json")

    assert response == 0
    assert f.exists()
    with f.open() as fp:
        lines = fp.readlines()
        assert lines == ["one\n", "two\n"]


def test_environment_vars(tmp_path, read_config):
    job_data_file = f"{tmp_path}/fake_job_data.json"

    commands = [
        f'if [ "$BC_WORKSPACE" != "{tmp_path}" ]; then exit 1; fi',
        f'if [ "$BC_JOB_DATA_FILE" != "{job_data_file}" ]; then exit 2; fi',
    ]

    os.chdir(str(tmp_path))

    response = run_commands(commands, read_config, str(tmp_path), job_data_file)
    assert response != 1, "BC_WORKSPACE environment variable incorrectly set"
    assert response != 2, "BC_JOB_DATA_FILE environment variable incorrectly set"
    assert response == 0


def test_exit_on_command_fail(tmp_path, read_config):
    f = tmp_path / "test_exit_on_command_fail.out"

    commands = [
        f"echo 'one' > {str(f)}",
        "false",
        f"echo 'two' >> {str(f)}"
    ]

    os.chdir(tmp_path)
    response = run_commands(commands, read_config, str(tmp_path), f"{tmp_path}/fake_job_data.json")
    assert response != 0

    assert f.exists()
    with f.open() as fp:
        lines = fp.readlines()
        assert lines == ["one\n"]


def test_exit_on_undef_var(tmp_path, read_config):
    f = tmp_path / "test_exit_on_undef_var.out"

    commands = [
        f"echo 'one' > {str(f)}",
        "echo $UNDEFINED_VAR",
        f"echo 'two' >> {str(f)}"
    ]

    os.chdir(tmp_path)
    response = run_commands(commands, read_config, str(tmp_path), f"{tmp_path}/fake_job_data.json")

    assert response != 0
    assert f.exists()
    with f.open() as fp:
        lines = fp.readlines()
        assert lines == ["one\n"]


@pytest.mark.skipif(os.environ.get("SHELL_NAME", "") == "sh", reason="can't do this in Bourne shell")
def test_pipefail(tmp_path, read_config):
    f = tmp_path / "test_pipefail.out"

    commands = [
        f"echo 'one' > {str(f)}",
        f"echo 'eh' | false | echo 'bee' >> {str(f)}",
        f"echo 'two' >> {str(f)}"
    ]

    os.chdir(tmp_path)
    response = run_commands(commands, read_config, str(tmp_path), f"{tmp_path}/fake_job_data.json")

    assert response != 0
    assert f.exists()
    with f.open() as fp:
        lines = fp.readlines()
        assert lines == ["one\n", "bee\n"]


@pytest.mark.skipif(os.environ.get("SHELL_NAME", "") == "sh", reason="can't do this in Bourne shell")
def test_subshell_fail(tmp_path, read_config):
    f = tmp_path / "test_subshell_fail.out"

    commands = [
        f"echo 'one' > {str(f)}",
        "foo=$(false; echo 'two')",
        f"echo $foo >> {str(f)}"
    ]

    os.chdir(tmp_path)
    response = run_commands(commands, read_config, str(tmp_path), f"{tmp_path}/fake_job_data.json")

    assert response != 0
    assert f.exists()
    with f.open() as fp:
        lines = fp.readlines()
        assert lines == ["one\n"]
