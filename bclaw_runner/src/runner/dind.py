from base64 import b64decode
from contextlib import closing
import logging
import os
import re
from typing import Generator

import boto3
import docker
from docker.errors import ImageNotFound
from docker.models.images import Image
from docker.types import DeviceRequest, DriverConfig, Mount
import requests

from .signal_trapper import signal_trapper

logger = logging.getLogger(__name__)

# https://docker-py.readthedocs.io/en/stable/index.html


def get_gpu_requests() -> list:
    if devices := os.environ.get("NVIDIA_VISIBLE_DEVICES"):
        logger.info(f"visible gpus: {devices}")
        if devices == "all":
            ret = [DeviceRequest(count=-1, capabilities=[["compute", "utility"]])]
        else:
            ret = [DeviceRequest(device_ids=devices.split(","), capabilities=[["compute", "utility"]])]
    else:
        ret = []

    return ret


def get_container_metadata() -> dict:
    url = os.environ.get("ECS_CONTAINER_METADATA_URI_V4")
    ret = requests.get(url).json()

    return ret


def get_mounts(metadata: dict, parent_workspace: str, child_workspace: str) -> Generator[Mount, None, None]:
    for volume_spec in metadata["Volumes"]:
        if "Source" in volume_spec:
            if volume_spec["Source"] == "/var/run/docker.sock":
                # prevent docker in docker in docker in docker...
                continue

            elif volume_spec["Destination"] == os.environ["BC_SCRATCH_PATH"]:
                # make only the workspace dir available to the child container, not the whole scratch volume

                # input:
                #   os.environment[BC_SCRATCH_PATH] = /_bclaw_scratch
                #   parent_workspace = /_bclaw_scratch/tmp12345
                #   child_workspace = /tmp12345
                #   volume_spec = { Source: /scratch
                #                   Destination: /_bclaw_scratch }
                # desired output: Mount(target=/tmp12345, source=/scratch/tmp12345, type="bind", read_only=False

                # first locate the parent workspace on the host's scratch volume, e.g.
                #   /_bclaw_scratch/tmp12345 -> /scratch/tmp12345
                host_workspace = parent_workspace.replace(os.environ["BC_SCRATCH_PATH"], volume_spec["Source"])

                # then mount the host path to the child container
                yield Mount(child_workspace, host_workspace, type="bind", read_only=False)

            else:
                yield Mount(volume_spec["Destination"], volume_spec["Source"], type="bind", read_only=True)

        elif "DockerName" in volume_spec:
            # this handles per-job EFS mounts
            yield Mount(volume_spec["Destination"], volume_spec["DockerName"], type="volume", read_only=True,
                        driver_config=DriverConfig("amazon-ecs-volume-plugin"))


def get_environment_vars() -> dict:
    # copy all environment variables starting with AWS_, BC_, or ECS_ to the child container
    ret = {k: v for k, v in os.environ.items() if re.match(r"^(?:AWS|BC|ECS)_.*", k)}
    return ret


def pull_image(docker_client: docker.DockerClient, tag: str) -> Image:
    try:
        # check if the image already exists locally
        ret = docker_client.images.get(tag)

    except ImageNotFound:
        if m := re.match(r"(\d+)\.dkr\.ecr", tag):
            # pull from ECR
            ecr_client = boto3.client("ecr")
            token = ecr_client.get_authorization_token(registryIds=m.groups())
            u, p = b64decode(token["authorizationData"][0]["authorizationToken"]).decode("utf-8").split(":")
            auth_config = {"username": u, "password": p}
        else:
            # pull from public repository
            auth_config = None

        ret = docker_client.images.pull(tag, auth_config=auth_config)

    return ret


def run_child_container(image_tag: str, command: str, parent_workspace: str, parent_job_data_file: str) -> int:
    child_workspace = os.environ["BC_SCRATCH_PATH"]

    parent_metadata = get_container_metadata()
    mounts = list(get_mounts(parent_metadata, parent_workspace, child_workspace))
    cpu_shares = parent_metadata["Limits"]["CPU"]
    mem_limit = f"{parent_metadata['Limits']['Memory']}m"

    environment = get_environment_vars()
    environment["BC_WORKSPACE"] = child_workspace
    environment["BC_JOB_DATA_FILE"] = os.path.join(child_workspace, os.path.basename(parent_job_data_file))

    device_requests = get_gpu_requests()

    with closing(docker.client.from_env()) as docker_client:
        child_image = pull_image(docker_client, image_tag)
        with signal_trapper():
            container = docker_client.containers.run(child_image.tags[0], command,
                                                     cpu_shares=cpu_shares,
                                                     detach=True,
                                                     device_requests=device_requests,
                                                     entrypoint=[],
                                                     environment=environment,
                                                     init=True,
                                                     mem_limit=mem_limit,
                                                     mounts=mounts,
                                                     version="auto",
                                                     working_dir=child_workspace)
            try:
                with closing(container.log(stream=True)) as fp:
                    # todo: new logger for this
                    for line in fp:
                        logger.info(line.decode("utf-8"))

            except BaseException:
                # stop the child container before crashing
                logger.exception("failed: ")
                logger.warning("stopping child container")
                container.stop(timeout=5)

            finally:
                logger.info("waiting for child container")
                response = container.wait()
                logger.info("cleaning up")
                container.remove()
                ret = response.get("StatusCode", 0)  # todo: default to 1?
                return ret
