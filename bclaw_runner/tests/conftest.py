import io
from typing import Optional

import boto3
from docker.errors import ImageNotFound
import moto
import pytest


class MockImage:
    def __init__(self, name: str, source: str, auth: Optional[dict] = None):
        self.tags = [name]
        self.source = source
        self.auth = auth
        self.attrs = {
            "RepoDigests": [f"{name}@sha256:1234567890abcdef"],
        }


class MockImages:
    @staticmethod
    def get(img_uri: str) -> MockImage:
        if img_uri == "local/image":
            return MockImage(img_uri, "local repo")
        else:
            raise ImageNotFound("not found message")

    @staticmethod
    def pull(img_uri: str, auth_config: dict) -> MockImage:
        if "dkr.ecr" in img_uri:
            return MockImage(img_uri, "ecr", auth_config)
        else:
            if auth_config:
                return MockImage(img_uri, "private repo", auth_config)
            else:
                return MockImage(img_uri, "public repo")


class MockContainer:
    def __init__(self, exit_code):
        self.args = None
        self.kwargs = None
        self.exit_code = exit_code
        self.removed = False
        self.status = "created"

    def logs(self, *args, **kwargs) -> io.BytesIO:
        ret = io.BytesIO(b"line 1\nline 2\nline 3")
        return ret

    def stop(self, *args, **kwargs) -> None:
        self.exit_code = 99

    def wait(self, *args, **kwargs) -> dict:
        ret = {"StatusCode": self.exit_code}
        return ret

    def remove(self, *args, **kwargs) -> None:
        self.removed = True

    def reload(self):
        self.status = "running"


class FailingContainer(MockContainer):
    def __init__(self, exit_code: int):
        super().__init__(exit_code)

    def logs(self, *args, **kwargs) -> io.BytesIO:
        raise RuntimeError("hey")


@pytest.fixture(scope="function")
def mock_container_factory():
    def _ret(exit_code: int, logging_crash: bool):
        if logging_crash:
            return FailingContainer(exit_code)
        else:
            return MockContainer(exit_code)
    return _ret


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


@pytest.fixture(scope="function")
def mock_docker_client_factory():
    def _ret(container: Optional[MockContainer] = None):
        return MockDockerClient(container)
    return _ret


@pytest.fixture(scope="module")
def mock_ec2_instance():
    with moto.mock_aws():
        ec2 = boto3.resource("ec2", region_name="us-east-1")
        instances = ec2.create_instances(ImageId="ami-12345", MinCount=1, MaxCount=1)
        yield instances[0]
