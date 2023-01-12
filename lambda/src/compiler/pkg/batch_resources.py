import json
import logging
import math
import os
import re
from typing import Generator, List, Tuple, Union

import humanfriendly

from .qc_resources import handle_qc_check
from .util import Step, Resource, State, make_logical_name, do_param_substitution, time_string_to_seconds

SCRATCH_PATH = "/_bclaw_scratch"


# "registry/path/image_name:version" -> ("registry/path", "image_name", "version")
# "registry/path/image_name"         -> ("registry/path", "image_name", None)
# "image_name:version"               -> (None, "image_name", "version")
# "image_name"                       -> (None, "image_name", None)
URI_PARSER = re.compile(r"^(?:(.+)/)?([^:]+)(?::(.+))?$")

def expand_image_uri(uri: str) -> Union[str, dict]:
    registry, image, version = URI_PARSER.fullmatch(uri).groups()

    if registry is None:
        if version is None:
            tag = ""
        else:
            tag = ":" + re.sub(r"\${", "${!", version)
        ret = {
            "Fn::Sub": f"${{AWS::AccountId}}.dkr.ecr.${{AWS::Region}}.amazonaws.com/{image}{tag}",
        }
    else:
        ret = uri

    return ret


def get_job_queue(compute_spec: dict) -> str:
    if (queue_name := compute_spec.get("queue_name")) is not None:
        ret = f"arn:aws:batch:${{AWSRegion}}:${{AWSAccountId}}:job-queue/{queue_name}"
    elif compute_spec["spot"]:
        ret = os.environ["SPOT_QUEUE_ARN"]
    else:
        ret = os.environ["ON_DEMAND_QUEUE_ARN"]
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


def get_environment(step: Step) -> dict:
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

    if (gpu_str := str(step.spec["compute"]["gpu"])) != "0":
        rc.append({
            "Type": "GPU",
            "Value": gpu_str,
        })

    ret = {"ResourceRequirements": rc}
    return ret


def get_volume_info(step: Step) -> dict:
    volumes = [
        {
            "Name": "docker_socket",
            "Host": {
                "SourcePath": "/var/run/docker.sock",
            },
        },
        {
            "Name": "scratch",
            "Host": {
                "SourcePath": "/scratch",
            },
        },
        {
            "Name": "docker_scratch",
            "Host": {
                "SourcePath": "/docker_scratch"
            },
        }
    ]
    mount_points = [
        {
            "SourceVolume": "docker_socket",
            "ContainerPath": "/var/run/docker.sock",
            "ReadOnly": False,
        },
        {
            "SourceVolume": "scratch",
            "ContainerPath": SCRATCH_PATH,
            "ReadOnly": False,
        },
        {
            "SourceVolume": "docker_scratch",
            "ContainerPath": "/.scratch",
            "ReadOnly": False,
        }
    ]

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


def job_definition_rc(step: Step,
                      task_role: str,
                      shell_opt: str) -> Generator[Resource, None, str]:
    job_def_name = make_logical_name(f"{step.name}.job.def")

    job_def = {
        "Type": "AWS::Batch::JobDefinition",
        "UpdateReplacePolicy": "Retain",
        "Properties": {
            "Type": "container",
            "Parameters": {
                "workflow_name": {
                    "Ref": "AWS::StackName",
                },
                "repo": "rrr",
                "image": expand_image_uri(step.spec["image"]),
                "inputs": "iii",
                "references": "fff",
                "command": json.dumps(step.spec["commands"]),
                "outputs": "ooo",
                "shell": shell_opt,
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
                    "--shell", "Ref::shell",
                    "--skip", "Ref::skip",
                ],
                "Image": os.environ["RUNNER_REPO_URI"] + ":" + os.environ["SOURCE_VERSION"],
                "JobRoleArn": task_role,
                **get_environment(step),
                **get_resource_requirements(step),
                **get_volume_info(step),
            },
            "SchedulingPriority": 1,
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


def batch_step(step: Step,
               job_definition_name: str,
               scattered: bool,
               next_step_override: str = None,
               attempts: int = 3,
               interval: str = "3s",
               backoff_rate: float = 1.5) -> dict:
    skip_behavior = get_skip_behavior(step.spec)

    if scattered:
        job_name = "States.Format('{}__{}__{}', $$.Execution.Name, $$.State.Name, $.index)"
    else:
        job_name = "States.Format('{}__{}', $$.Execution.Name, $$.State.Name)"

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
            "JobName.$": job_name,
            "JobDefinition": f"${{{job_definition_name}}}",
            "JobQueue": get_job_queue(step.spec["compute"]),
            "ShareIdentifier": "${WorkflowName}",
            "Parameters": {
                "repo.$": "$.repo",
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


def handle_batch(step: Step,
                 wf_params: dict,
                 scattered: bool) -> Generator[Resource, None, List[State]]:
    logger = logging.getLogger(__name__)
    logger.info(f"making batch step {step.name}")

    task_role = step.spec.get("task_role") or wf_params.get("task_role") or os.environ["ECS_TASK_ROLE_ARN"]
    shell_opt = step.spec["compute"]["shell"] or wf_params.get("shell")

    subbed_spec = do_param_substitution(step.spec)
    subbed_step = Step(step.name, subbed_spec, step.next)

    job_def_name = yield from job_definition_rc(subbed_step, task_role, shell_opt)

    if subbed_spec["qc_check"] is not None:
        qc_state = handle_qc_check(subbed_step)
        ret0 = batch_step(subbed_step, job_def_name, scattered, **subbed_step.spec["retry"],
                          next_step_override=qc_state.name)
        ret = [State(step.name, ret0), qc_state]

    else:
        ret = [State(step.name, batch_step(subbed_step, job_def_name, **subbed_step.spec["retry"],
                                           scattered=scattered))]

    return ret
