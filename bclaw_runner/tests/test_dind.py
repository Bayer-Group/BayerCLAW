import io
import json
import pytest
from typing import Optional

import docker
from docker.errors import ImageNotFound
from docker.types import DeviceRequest, DriverConfig, Mount
import moto

from ..src.runner.dind import get_gpu_requests, get_container_metadata, get_mounts, get_environment_vars, pull_image, run_child_container


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
                "DockerName": "volume12345",
                "Destination": "/efs",
            },
            {
                "Source": "/miscellaneous/host/volume",
                "Destination": "/somewhere"
            }
        ]
    }
    parent_workspace = "/_bclaw_scratch/parent_workspace"
    child_workspace = "/child_workspace"

    expect = [
        Mount(child_workspace, "/scratch/parent_workspace", type="bind", read_only=False),
        Mount("/efs", "volume12345", type="volume", read_only=True,
              driver_config=DriverConfig("amazon-ecs-volume-plugin")),
        Mount("/somewhere", "/miscellaneous/host/volume", type="bind", read_only=True)
    ]

    result = list(get_mounts(metadata, parent_workspace, child_workspace))
    assert result == expect


def test_get_environment_vars(monkeypatch):
    monkeypatch.setenv("AWS_VARIABLE", "aws_value")
    monkeypatch.setenv("BC_VARIABLE", "bc_value")
    monkeypatch.setenv("ECS_VARIABLE", "ecs_value")
    monkeypatch.setenv("OTHER_VARIABLE", "other_value")
    result = get_environment_vars()

    expect = {
        "AWS_VARIABLE": "aws_value",
        "BC_VARIABLE": "bc_value",
        "ECS_VARIABLE": "ecs_value",
    }

    assert result == expect


class MockImage:
    def __init__(self, tag: str, source: str, auth: Optional[dict] = None):
        self.tags = [tag]
        self.source = source
        self.auth = auth


class MockImages:
    @staticmethod
    def get(tag: str) -> MockImage:
        if tag == "local/image":
            return MockImage(tag, "local repo")
        else:
            raise ImageNotFound("not found message")

    @staticmethod
    def pull(tag: str, auth_config: dict) -> MockImage:
        if auth_config:
            return MockImage(tag, "ecr", auth_config)
        else:
            return MockImage(tag, "public repo")


class MockContainer:
    def __init__(self):
        self.args = None
        self.kwargs = None
        self.exit_code = 0
        self.removed = False

    def log(self, *args, **kwargs) -> io.BytesIO:
        ret = io.BytesIO(b"line 1\nline 2\nline 3")
        return ret

    def stop(self, *args, **kwargs) -> None:
        self.exit_code = 99

    def wait(self, *args, **kwargs) -> dict:
        ret = {"StatusCode": self.exit_code}
        return ret

    def remove(self, *args, **kwargs) -> None:
        self.removed = True


class FailingContainer(MockContainer):
    def __init__(self):
        super().__init__()

    def log(self, *args, **kwargs) -> io.BytesIO:
        raise RuntimeError("hey")


class MockContainers:
    def __init__(self, ret: MockContainer):
        self.ret = ret

    def run(self, *args, **kwargs) -> MockContainer:
        self.ret.args = args
        self.ret.kwargs = kwargs
        return self.ret


class MockDockerClient():
    def __init__(self, container: Optional[MockContainer] = None):
        self.images = MockImages()
        self.containers = MockContainers(container)

    def close(self):
        pass


@pytest.mark.parametrize("tag, expected_source, expected_auth", [
    ("local/image", "local repo", None),
    ("public/image", "public repo", None),
    ("987654321.dkr.ecr.us-east-1.amazonaws.com/ecr-image", "ecr", {"username": "AWS", "password": "987654321-auth-token"}),
])
def test_pull_images(tag, expected_source, expected_auth, monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with moto.mock_ecr():
        client = MockDockerClient()
        result = pull_image(client, tag)
        assert result.tags == [tag]
        assert result.source == expected_source
        assert result.auth == expected_auth


@pytest.mark.parametrize("test_container, expected_result", [
    (MockContainer(), 0),
    (FailingContainer(), 99),
])
def test_run_child_container(monkeypatch, requests_mock, test_container, expected_result):
    bc_scratch_path = "/_bclaw_scratch"
    monkeypatch.setenv("BC_SCRATCH_PATH", bc_scratch_path)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("NVIDIA_VISIBLE_DEVICES", "all")
    monkeypatch.setenv("OTHER_VARIABLE", "other_value")

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

    def mock_from_env():
        return MockDockerClient(test_container)

    job_data_file = f"{bc_scratch_path}/parent/workspace/job_data_12345.json"

    monkeypatch.setattr(docker.client, "from_env", mock_from_env)
    result = run_child_container("local/image", "ls -l", f"{bc_scratch_path}/parent/workspace", job_data_file)

    assert test_container.args == ("local/image", "ls -l")
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
            "ECS_CONTAINER_METADATA_URI_V4": fake_url,
        },
        "init": True,
        "mem_limit": "2048m",
        "mounts": [Mount(bc_scratch_path, "/host/volume/scratch/parent/workspace", type="bind", read_only=False)],
        "version": "auto",
        "working_dir": bc_scratch_path
    }
    assert test_container.removed == True
    assert result == expected_result
