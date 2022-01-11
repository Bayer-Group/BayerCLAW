from functools import lru_cache
import json
import logging
import math
import re
from typing import Generator, List, Tuple, Union

import boto3
import humanfriendly

from .qc_resources import handle_qc_check
from .util import CoreStack, Step, Resource, State, make_logical_name, do_param_substitution,\
    time_string_to_seconds

SCRATCH_PATH = "/_bclaw_scratch"
EFS_PATH = "/mnt/efs"

# "registry/path/image_name:version" -> ("registry/path", "image_name:version", "image_name", "version")
# "registry/path/image_name"         -> ("registry/path", "image_name", "image_name", None)
# "image_name:version"               -> (None, "image_name:version", "image_name", "version")
# "image_name"                       -> (None, "image_name", "image_name", None)
URI_PARSER = re.compile(r"^(?:(.+)/)?(([^:]+)(?::(.+))?)$")

def parse_uri(uri: str) -> Tuple[str, str, str, str]:
    registry, image_version, image, version = URI_PARSER.fullmatch(uri).groups()
    return registry, image_version, image, version


def get_ecr_uri(registry: Union[str, None], image_version: str) -> Union[str, dict]:
    if registry is None:
        ret = {
            "Fn::Sub": f"${{AWS::AccountId}}.dkr.ecr.${{AWS::Region}}.amazonaws.com/{image_version}",
        }
    else:
        ret = "/".join([registry, image_version])

    return ret


@lru_cache(maxsize=None)
def get_custom_job_queue_arn(queue_name: str) -> str:
    batch = boto3.client("batch")
    desc = batch.describe_job_queues(jobQueues=[queue_name])
    ret = desc["jobQueues"][0]["jobQueueArn"]
    return ret


def get_job_queue(core_stack: CoreStack, compute_spec: dict) -> str:
    if compute_spec.get("queue_name") is not None:
        ret = get_custom_job_queue_arn(compute_spec["queue_name"])
    elif compute_spec["spot"]:
        ret = core_stack.output("SpotQueueArn")
    else:
        ret = core_stack.output("OnDemandQueueArn")
    return ret


def get_memory_in_mibs(request: Union[str, float, int]) -> int:
    if isinstance(request, (float, int)):
        mibs_requested = math.ceil(request)

    else:
        # only str, float, and int will pass validation, so this must be a str
        n_bytes = humanfriendly.parse_size(request, binary=True)
        mibs_requested = math.ceil(n_bytes / 1048576)  # 1048576 = 1 MiB

    ret = max(4, mibs_requested)  # AWS Batch requires at least 4 MiB
    return ret


def get_environment(step: Step, global_efs_id: str) -> dict:
    vars = [
        {
            "Name": "BC_WORKFLOW_NAME",
            "Value": {"Ref": "AWS::StackName"},
        },
        {
            "Name": "BC_SCRATCH_PATH",
            "Value": SCRATCH_PATH,
        },
        {
            "Name": "BC_STEP_NAME",
            "Value": step.name,
        },
        {
            "Name": "AWS_DEFAULT_REGION",
            "Value": {"Ref": "AWS::Region"},
        },
    ]

    if global_efs_id.startswith("fs-"):
        vars.append({
            "Name": "BC_EFS_PATH",
            "Value": EFS_PATH,
        })

    ret = {"Environment": vars}
    return ret


def get_resource_requirements(step: Step) -> dict:
    rc = [
        {
            "Type": "VCPU",
            "Value": str(step.spec["compute"]["cpus"]),
        },
        {
            "Type": "MEMORY",
            "Value": str(get_memory_in_mibs(step.spec["compute"]["memory"])),
        },
    ]

    if step.spec["compute"]["gpu"] > 0:
        rc.append({
            "Type": "GPU",
            "Value": str(step.spec["compute"]["gpu"])
        })

    ret = {"ResourceRequirements": rc}
    return ret


def get_volume_info(step: Step, global_efs_id: str) -> dict:
    volumes = [
        {
            "Name": "docker_scratch",
            "Host": {
                "SourcePath": "/docker_scratch",
            },
        },
        {
            "Name": "scratch",
            "Host": {
                "SourcePath": "/scratch",
            },
        }]
    mount_points = [
        {
            "SourceVolume": "docker_scratch",
            "ContainerPath": "/scratch",
            "ReadOnly": False,
        },
        {
            "SourceVolume": "scratch",
            "ContainerPath": SCRATCH_PATH,
            "ReadOnly": False,
        }]

    if global_efs_id.startswith("fs-"):
        volumes.append({
            "Name": "efs",
            "Host": {
                "SourcePath": EFS_PATH,
            },
        })
        mount_points.append({
            "SourceVolume": "efs",
            "ContainerPath": EFS_PATH,
            "ReadOnly": True,
        })

    for filesystem in step.spec["filesystems"]:
        volume_name = f"{filesystem['efs_id']}-volume"
        volumes.append({
            "Name": volume_name,
            "EfsVolumeConfiguration": {
                "FileSystemId": filesystem["efs_id"],
                "RootDirectory": filesystem["root_dir"],
                "TransitEncryption": "ENABLED",
            },
        })
        mount_points.append({
            "SourceVolume": volume_name,
            "ContainerPath": filesystem["host_path"],
            "ReadOnly": True,
        })

    ret = {
        "Volumes": volumes,
        "MountPoints": mount_points,
    }

    return ret


def get_timeout(step: Step) -> dict:
    if step.spec.get("timeout") is None:
        ret = {}
    else:
        ret = {"Timeout": {"AttemptDurationSeconds": max(time_string_to_seconds(step.spec["timeout"]), 60)}}
    return ret


def job_definition_rc(core_stack: CoreStack,
                      step: Step,
                      task_role: Union[str, dict]) -> Generator[Resource, None, str]:
    job_def_name = make_logical_name(f"{step.name}.job.def")

    registry, image_version, image, version = parse_uri(step.spec["image"])

    global_efs_volume_id = core_stack.output("EFSVolumeId")

    job_def = {
        "Type": "AWS::Batch::JobDefinition",
        "Properties": {
            "Type": "container",
            "Parameters": {
                "workflow_name": {
                    "Ref": "AWS::StackName",
                },
                "repo": "rrr",
                "image": get_ecr_uri(registry, image_version),
                "inputs": "iii",
                "references": "fff",
                "command": json.dumps(step.spec["commands"]),
                "outputs": "ooo",
                "skip": "sss",
            },
            "ContainerProperties": {
                "Command": [
                    "python", "/bclaw_runner/src/runner_cli.py",
                    "--repo", "Ref::repo",
                    "--image", "Ref::image",
                    "--in", "Ref::inputs",
                    "--ref", "Ref::references",
                    "--cmd", "Ref::command",
                    "--out", "Ref::outputs",
                    "--skip", "Ref::skip",
                ],
                "Image": core_stack.output("RunnerImageURI"),
                "JobRoleArn": task_role,
                **get_environment(step, global_efs_volume_id),
                **get_resource_requirements(step),
                **get_volume_info(step, global_efs_volume_id),
            },
            **get_timeout(step)
        },
    }

    yield Resource(job_def_name, job_def)
    return job_def_name


def get_skip_behavior(spec: dict) -> str:
    if "skip_if_output_exists" in spec and spec["skip_if_output_exists"]:
        ret = "output"
    elif "skip_on_rerun" in spec and spec["skip_on_rerun"]:
        ret = "rerun"
    else:
        ret = "none"

    return ret


def batch_step(core_stack: CoreStack,
               step: Step,
               job_definition_name: str,
               next_step_override: str = None,
               attempts: int = 3,
               interval: str = "3s",
               backoff_rate: float = 1.5) -> dict:
    skip_behavior = get_skip_behavior(step.spec)

    ret = {
        "Type": "Task",
        "Resource": "arn:aws:states:::batch:submitJob.sync",
        "Retry": [
            {
                "ErrorEquals": ["States.ALL"],
                "IntervalSeconds": time_string_to_seconds(interval),
                "MaxAttempts": attempts,
                "BackoffRate": backoff_rate
            }
        ],
        "Parameters": {
            "JobName.$": "States.Format('{}__{}__{}__{}', $$.StateMachine.Name, $$.State.Name, $.id_prefix, $.index)",
            "JobDefinition": f"${{{job_definition_name}}}",
            "JobQueue": get_job_queue(core_stack, step.spec["compute"]),
            "Parameters": {
                "repo.$": "$.repo",
                # "parameters": json.dumps(spec["params"]),
                **step.input_field,
                "references": json.dumps(step.spec["references"]),
                "outputs": json.dumps(step.spec["outputs"]),
                "skip": skip_behavior,
            },
            "ContainerOverrides": {
                "Environment": [
                    {
                        "Name": "BC_BRANCH_IDX",
                        "Value.$": "$.index",
                    },
                    {
                        "Name": "BC_EXECUTION_ID",
                        "Value.$": "$$.Execution.Name",
                    },
                    {
                        "Name": "BC_LAUNCH_BUCKET",
                        "Value.$": "$.job_file.bucket",
                    },
                    {
                        "Name": "BC_LAUNCH_KEY",
                        "Value.$": "$.job_file.key",
                    },
                    {
                        "Name": "BC_LAUNCH_VERSION",
                        "Value.$": "$.job_file.version",
                    },
                    {
                        "Name": "BC_LAUNCH_S3_REQUEST_ID",
                        "Value.$": "$.job_file.s3_request_id",
                    },
                ],
            },
        },
        "ResultSelector": {
            **step.spec["outputs"],
        },
        "ResultPath": "$.prev_outputs",
        "OutputPath": "$",
    }

    if next_step_override is None:
        ret.update(**step.next_or_end)
    else:
        ret.update({"Next": next_step_override})

    return ret


def handle_batch(core_stack: CoreStack,
                 step: Step,
                 wf_params: dict) -> Generator[Resource, None, List[State]]:
    logger = logging.getLogger(__name__)
    logger.info(f"making batch step {step.name}")

    task_role = step.spec.get("task_role") or wf_params.get("task_role") or core_stack.output("ECSTaskRoleArn")

    subbed_spec = do_param_substitution(step.spec)
    subbed_step = Step(step.name, subbed_spec, step.next)

    job_def_name = yield from job_definition_rc(core_stack, subbed_step, task_role)

    if subbed_spec["qc_check"] is not None:
        qc_state = handle_qc_check(core_stack, subbed_step)
        ret0 = batch_step(core_stack, subbed_step, job_def_name, **subbed_step.spec["retry"],
                          next_step_override=qc_state.name)
        ret = [State(step.name, ret0), qc_state]

    else:
        ret = [State(step.name, batch_step(core_stack, subbed_step, job_def_name, **subbed_step.spec["retry"]))]

    return ret
