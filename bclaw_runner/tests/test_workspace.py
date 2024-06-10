import os
import json
import subprocess

import pytest

from ..src import runner
from ..src.runner.workspace import workspace, write_job_data_file, run_commands, run_commands, UserCommandsFailed


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


def fake_container(image_tag: str, command: str, work_dir: str, job_data_file) -> int:
    response = subprocess.run(command, shell=True)
    return response.returncode


def test_run_commands(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(runner.workspace, "run_child_container", fake_container)
    f = tmp_path / "test_success.out"

    commands = [
        f"echo 'one' > {str(f)}",
        "z='two'",
        f"echo $z >> {str(f)}"
    ]

    os.chdir(tmp_path)
    response = run_commands("fake/image:tag", commands, tmp_path, "fake/job/data/file.json", "sh")

    assert "command block succeeded" in caplog.text
    assert f.exists()
    with f.open() as fp:
        lines = fp.readlines()
        assert lines == ["one\n", "two\n"]


def test_exit_on_command_fail1(tmp_path, monkeypatch):
    monkeypatch.setattr(runner.workspace, "run_child_container", fake_container)
    f = tmp_path / "test_exit_on_command_fail.out"

    commands = [
        f"echo 'one' > {str(f)}",
        "false",
        f"echo $z >> {str(f)}"
    ]

    os.chdir(tmp_path)
    with pytest.raises(UserCommandsFailed) as ucf:
        run_commands("fake/image:tag", commands, tmp_path, "fake/job/data/file.json", "sh")
        assert ucf.value.exit_code != 0

    assert f.exists()
    with f.open() as fp:
        lines = fp.readlines()
        assert lines == ["one\n"]


def test_exit_on_undef_var1(tmp_path, monkeypatch):
    monkeypatch.setattr(runner.workspace, "run_child_container", fake_container)
    f = tmp_path / "test_exit_on_undef_var.out"

    commands = [
        f"echo 'one' > {str(f)}",
        "echo $UNDEFINED_VAR",
        f"echo $z >> {str(f)}"
    ]

    os.chdir(tmp_path)
    with pytest.raises(UserCommandsFailed) as ucf:
        run_commands("fake/image:tag", commands, tmp_path, "fake/job/data/file.json", "sh")
        assert ucf.value.exit_code != 0

    assert f.exists()
    with f.open() as fp:
        lines = fp.readlines()
        assert lines == ["one\n"]
