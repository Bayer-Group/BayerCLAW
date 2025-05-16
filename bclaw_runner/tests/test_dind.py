import json
import pytest

import boto3
import docker
from docker.types import DeviceRequest, DriverConfig, Mount
import moto

from ..src.runner.dind import (get_gpu_requests, get_container_metadata, get_mounts, get_environment_vars, get_auth,
                               pull_image, run_child_container)


TEST_SECRET_NAME = "test_secret"
TEST_SECRET = {
    "username": "me",
    "password": "my_password"
}

@pytest.fixture(scope="function")
def mock_secrets():
    with moto.mock_aws():
        secrets = boto3.client("secretsmanager", region_name="us-east-1")
        secrets.create_secret(Name=TEST_SECRET_NAME, SecretString=json.dumps(TEST_SECRET))
        yield TEST_SECRET_NAME


@pytest.mark.parametrize("nvidia_visible_devices, expect", [
    (None, []),
    ("all", [DeviceRequest(count=-1, capabilities=[["compute", "utility"]])]),
    ("gpu-12345", [DeviceRequest(device_ids=["gpu-12345"], capabilities=[["compute", "utility"]])]),
    ("gpu-12345,gpu-54321", [DeviceRequest(device_ids=["gpu-12345", "gpu-54321"], capabilities=[["compute", "utility"]])])
])
def test_get_gpu_requests(nvidia_visible_devices, expect, monkeypatch):
    if nvidia_visible_devices is not None:
        monkeypatch.setenv("NVIDIA_VISIBLE_DEVICES", nvidia_visible_devices)
    result = get_gpu_requests()
    assert result == expect


def test_get_container_metadata(monkeypatch, requests_mock):
    fake_url = "http://169.254.99.99/container_metadata"
    monkeypatch.setenv("ECS_CONTAINER_METADATA_URI_V4", fake_url)
    metadata = {"meta": "data"}
    requests_mock.get(fake_url, text=json.dumps(metadata))

    result = get_container_metadata()
    assert result == metadata


def test_get_mounts(monkeypatch):
    monkeypatch.setenv("BC_SCRATCH_PATH", "/_bclaw_scratch")
    metadata = {
        "Volumes": [
            {
                "Source": "/var/run/docker.sock",
                "Destination": "/var/run/docker.sock",
            },
            {
                "Source": "/scratch",
                "Destination": "/_bclaw_scratch",
            },
            {
                "Source": "/docker_scratch",
                "Destination": "/.scratch",
            },
            {
                "DockerName": "volume12345",
                "Destination": "/efs",
            },
            {
                "Source": "/miscellaneous/host/volume",
                "Destination": "/somewhere",
            },
        ],
    }
    parent_workspace = "/_bclaw_scratch/parent_workspace"
    child_workspace = "/child_workspace"

    expect = [
        Mount(child_workspace, "/scratch/parent_workspace", type="bind", read_only=False),
        Mount("/.scratch", "/docker_scratch", type="bind", read_only=False),
        Mount("/efs", "volume12345", type="volume", read_only=False,
              driver_config=DriverConfig("amazon-ecs-volume-plugin")),
        Mount("/somewhere", "/miscellaneous/host/volume", type="bind", read_only=False)
    ]

    result = list(get_mounts(metadata, parent_workspace, child_workspace))
    assert result == expect


def test_get_environment_vars(monkeypatch):
    monkeypatch.setenv("AWS_VARIABLE", "aws_value")
    monkeypatch.setenv("BC_VARIABLE", "bc_value")
    monkeypatch.setenv("ECS_VARIABLE", "ecs_value")
    monkeypatch.setenv("OTHER_VARIABLE", "other_value")
    monkeypatch.delenv("AWS_CA_BUNDLE", raising=False)
    result = get_environment_vars()

    expect = {
        "AWS_VARIABLE": "aws_value",
        "BC_VARIABLE": "bc_value",
    }

    assert result == expect


def test_get_auth(mock_secrets):
    result = get_auth(mock_secrets)
    assert result == TEST_SECRET


@pytest.mark.parametrize("image_spec, expected_source, expected_auth", [
    ({"name": "public/image", "auth": ""}, "public repo", None),
    ({"name": "private/image", "auth": TEST_SECRET_NAME}, "private repo", TEST_SECRET),
    ({"name": "987654321.dkr.ecr.us-east-1.amazonaws.com/ecr-image", "auth": ""}, "ecr", {"username": "AWS", "password": "987654321-auth-token"}),
])
def test_pull_image(image_spec, expected_source, expected_auth, monkeypatch, mock_docker_client_factory, mock_secrets):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    with moto.mock_aws():
        client = mock_docker_client_factory()
        result = pull_image(client, image_spec)
        assert image_spec["name"] in result.tags
        assert result.source == expected_source
        assert result.auth == expected_auth


@pytest.mark.parametrize("exit_code", [0, 88])
@pytest.mark.parametrize("logging_crash", [False, True])
def test_run_child_container(caplog, monkeypatch, requests_mock, exit_code, logging_crash,
                             mock_container_factory, mock_docker_client_factory):
    bc_scratch_path = "/_bclaw_scratch"
    monkeypatch.setenv("BC_SCRATCH_PATH", bc_scratch_path)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("NVIDIA_VISIBLE_DEVICES", "all")
    monkeypatch.setenv("OTHER_VARIABLE", "other_value")
    monkeypatch.delenv("AWS_CA_BUNDLE", raising=False)

    metadata = {
        "Volumes": [
            {
                "Source": "/var/run/docker.sock",
                "Destination": "/var/run/docker.sock",
            },
            {
                "Source": "/host/volume/scratch",
                "Destination": bc_scratch_path,
            },
        ],
        "Limits": {
            "CPU": 1024,
            "Memory": 2048,
        }
    }

    fake_url = "http://169.254.99.99/container_metadata"
    monkeypatch.setenv("ECS_CONTAINER_METADATA_URI_V4", fake_url)
    requests_mock.get(fake_url, text=json.dumps(metadata))

    test_container = mock_container_factory(exit_code, logging_crash)

    def mock_from_env():
        return mock_docker_client_factory(test_container)
    monkeypatch.setattr(docker.client, "from_env", mock_from_env)

    job_data_file = f"{bc_scratch_path}/parent/workspace/job_data_12345.json"

    image_spec = {
        "name": "local/image",
        "auth": "",
    }
    result = run_child_container(image_spec, "ls -l", f"{bc_scratch_path}/parent/workspace", job_data_file)

    assert test_container.args[0].tags == ["local/image"]
    assert test_container.args[1] == "ls -l"
    assert test_container.kwargs == {
        "cpu_shares": 1024,
        "detach": True,
        "device_requests": [DeviceRequest(count=-1, capabilities=[["compute", "utility"]])],
        "entrypoint": [],
        "environment": {
            "AWS_DEFAULT_REGION": "us-east-1",
            "BC_JOB_DATA_FILE": f"{bc_scratch_path}/job_data_12345.json",
            "BC_SCRATCH_PATH": bc_scratch_path,
            "BC_WORKSPACE": bc_scratch_path,
        },
        "init": True,
        "mem_limit": "2048m",
        "mounts": [Mount(bc_scratch_path, "/host/volume/scratch/parent/workspace", type="bind", read_only=False)],
        "version": "auto",
        "working_dir": bc_scratch_path
    }
    assert test_container.removed is True
    assert result == exit_code

    if logging_crash:
        assert "continuing without subprocess logging" in caplog.text
