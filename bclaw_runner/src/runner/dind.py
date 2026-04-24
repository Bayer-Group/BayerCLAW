from base64 import b64decode
from contextlib import closing
import json
import logging
import os
import re
from typing import Generator

import boto3
import docker
from docker.models.images import Image
from docker.types import DeviceRequest, DriverConfig, Mount
import requests

from .inline_cmds import UserDefinedError, parse_for_commands
from .signal_trapper import signal_trapper
from .workspace import Workspace

logger = logging.getLogger(__name__)

user_handler = logging.StreamHandler()
user_formatter = logging.Formatter("%(levelname)s: %(message)s")
user_handler.setFormatter(user_formatter)
user_cmd_logger = logging.getLogger("user_cmd")
user_cmd_logger.addHandler(user_handler)
user_cmd_logger.propagate = False

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


def get_mounts(metadata: dict, workspace: Workspace) -> Generator[Mount, None, None]:
    for volume_spec in metadata["Volumes"]:
        logger.info(f"{volume_spec=}")

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
                # desired output: Mount(target=/tmp12345, source=/scratch/tmp12345, type="bind", read_only=False)

                # first locate the parent workspace on the host's scratch volume, e.g.
                #   /_bclaw_scratch/tmp12345 -> /scratch/tmp12345
                # host_workspace = parent_workspace.replace(os.environ["BC_SCRATCH_PATH"], volume_spec["Source"])
                host_workspace = str(workspace.host_path)

                # then mount the host path to the child container
                yield Mount(str(workspace.child_path), host_workspace, type="bind", read_only=False)

            elif volume_spec["Destination"] == "/.scratch":
                yield Mount(volume_spec["Destination"], volume_spec["Source"], type="bind", read_only=False)

            else:
                yield Mount(volume_spec["Destination"], volume_spec["Source"], type="bind", read_only=False)

        elif "DockerName" in volume_spec:
            # this handles per-job EFS mounts
            # template:
            #   efs_id: fs-b5a4dd01
            #   host_path: /efs
            # volume_spec:
            #   {'DockerName': 'ecs-jax-efs-test-EfsTestJobDef--1-1-fs-b5a4dd01-volume-bcd3a18fd8e28a9fe901',
            #    'Destination': '/efs'}
            yield Mount(volume_spec["Destination"], volume_spec["DockerName"], type="volume", read_only=False,
                        driver_config=DriverConfig("amazon-ecs-volume-plugin"))


def get_environment_vars() -> dict:
    # copy all environment variables starting with AWS_ or BC_ to the child container
    ret = {k: v for k, v in os.environ.items() if re.match(r"^(?:AWS|BC)_.*", k)}
    return ret


def get_auth(secret_id: str) -> dict:
    logger.info("getting docker credentials from secrets manager")
    client = boto3.client("secretsmanager")
    secret = client.get_secret_value(SecretId=secret_id)

    # secret should be a json string containing "username" and "password" keys
    ret = json.loads(secret["SecretString"])
    return ret


def pull_image(docker_client: docker.DockerClient, image_spec: dict) -> Image:
    img_repo = image_spec["name"]
    if m := re.match(r"(\d+)\.dkr\.ecr", img_repo):
        # pull from ECR
        logger.info(f"pulling image {img_repo} from ECR")
        ecr_client = boto3.client("ecr")
        token = ecr_client.get_authorization_token(registryIds=m.groups())
        u, p = b64decode(token["authorizationData"][0]["authorizationToken"]).decode("utf-8").split(":")
        auth_config = {"username": u, "password": p}
    else:
        if image_spec["auth"]:
            logger.info(f"pulling image {img_repo} from private repo")
            auth_config = get_auth(image_spec["auth"])
        else:
            logger.info(f"pulling image {img_repo} from public repo")
            auth_config = None

    ret = docker_client.images.pull(img_repo, auth_config=auth_config)

    repo_id = ret.attrs["RepoDigests"][0].split("@")[-1][:19]
    logger.info(f"got image {ret.tags[0]} ({repo_id})")

    return ret


def run_child_container(image_spec: dict, command: str, workspace: Workspace) -> int:
    # child_workspace = os.environ["BC_SCRATCH_PATH"]

    parent_metadata = get_container_metadata()
    mounts = list(get_mounts(parent_metadata, workspace))
    cpu_shares = parent_metadata["Limits"]["CPU"]
    mem_limit = f"{parent_metadata['Limits']['Memory']}m"

    environment = get_environment_vars()
    environment["BC_WORKSPACE"] = str(workspace.child_path)
    # environment["BC_JOB_DATA_FILE"] = os.path.join(child_workspace, os.path.basename(parent_job_data_file))

    device_requests = get_gpu_requests()

    exit_code = 255
    with closing(docker.client.from_env()) as docker_client:
        child_image = pull_image(docker_client, image_spec)

        logger.info("---------- starting user command block ----------")
        container = docker_client.containers.run(child_image, command,
                                                 cpu_shares=cpu_shares,
                                                 detach=True,
                                                 device_requests=device_requests,
                                                 entrypoint=[],
                                                 environment=environment,
                                                 init=True,
                                                 mem_limit=mem_limit,
                                                 mounts=mounts,
                                                 version="auto",
                                                 working_dir=str(workspace.child_path))
        with signal_trapper(container):
            try:
                with closing(container.logs(stream=True)) as fp:
                    for line in fp:
                        line_str = line.decode("utf-8")
                        user_cmd_logger.user_cmd(line_str)
                        parse_for_commands(line_str)

            except UserDefinedError:
                raise

            except Exception:
                logger.exception("----- error during subprocess logging: ")
                container.reload()
                logger.info(f"----- subprocess status is {container.status}")
                logger.warning("----- continuing without subprocess logging")

            finally:
                logger.info("---------- end of user command block ----------")
                response = container.wait()
                container.remove()
                exit_code = response.get("StatusCode", 1)
                logger.info(f"{exit_code=}")

    return exit_code
