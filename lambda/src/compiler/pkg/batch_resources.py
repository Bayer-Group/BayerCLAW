import json
import logging
import math
import os
import re
from typing import Generator, List, Union

import humanfriendly

from .qc_resources import handle_qc_check
from .util import Step, Resource, State, make_logical_name, time_string_to_seconds

SCRATCH_PATH = "/_bclaw_scratch"


def expand_image_uri(uri: str) -> Union[str, dict]:
    subbed = re.sub(r"\${", "${!", uri)
    # https://stackoverflow.com/questions/37861791/how-are-docker-image-names-parsed
    #   The hostname [of a docker image uri] must contain a . dns separator,
    #   a : port separator, or the value "localhost" before the first /.
    # ...but it's unlikely that localhost will be used in a batch job
    if re.match(r"^.*[.:].*/", subbed):
        return subbed
    else:
        return {"Fn::Sub": f"${{AWS::AccountId}}.dkr.ecr.${{AWS::Region}}.amazonaws.com/{subbed}"}


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


def get_environment() -> dict:
    ret = {
        "environment": [
            {
                "name": "BC_SCRATCH_PATH",
                "value": SCRATCH_PATH,
            },
        ]
    }
    return ret


def get_resource_requirements(step: Step) -> dict:
    rc = [
        {
            "type": "VCPU",
            "value": str(step.spec["compute"]["cpus"]),
        },
        {
            "type": "MEMORY",
            "value": str(get_memory_in_mibs(step.spec["compute"]["memory"])),
        },
    ]

    if (gpu_str := str(step.spec["compute"]["gpu"])) != "0":
        rc.append({
            "type": "GPU",
            "value": gpu_str,
        })

    ret = {"resourceRequirements": rc}
    return ret


def get_volume_info(step: Step) -> dict:
    volumes = [
        {
            "name": "docker_socket",
            "host": {
                "sourcePath": "/var/run/docker.sock",
            },
        },
        {
            "name": "scratch",
            "host": {
                "sourcePath": "/scratch",
            },
        },
        {
            "name": "docker_scratch",
            "host": {
                "sourcePath": "/docker_scratch"
            },
        }
    ]
    mount_points = [
        {
            "sourceVolume": "docker_socket",
            "containerPath": "/var/run/docker.sock",
            "readOnly": False,
        },
        {
            "sourceVolume": "scratch",
            "containerPath": SCRATCH_PATH,
            "readOnly": False,
        },
        {
            "sourceVolume": "docker_scratch",
            "containerPath": "/.scratch",
            "readOnly": False,
        }
    ]

    for filesystem in step.spec["filesystems"]:
        volume_name = f"{filesystem['efs_id']}-volume"
        volumes.append({
            "name": volume_name,
            "efsVolumeConfiguration": {
                "fileSystemId": filesystem["efs_id"],
                "rootDirectory": filesystem["root_dir"],
                "transitEncryption": "ENABLED",
            },
        })
        mount_points.append({
            "sourceVolume": volume_name,
            "containerPath": filesystem["host_path"],
            "readOnly": False,
        })

    ret = {
        "volumes": volumes,
        "mountPoints": mount_points,
    }

    return ret


def get_timeout(step: Step) -> dict:
    if step.spec.get("timeout") is None:
        ret = {}
    else:
        ret = {"timeout": {"attemptDurationSeconds": max(time_string_to_seconds(step.spec["timeout"]), 60)}}
    return ret


def job_definition_rc(step: Step,
                      task_role: str,
                      shell_opt: str) -> Generator[Resource, None, str]:
    logical_name = make_logical_name(f"{step.name}.job.defx")

    # todo: test this
    if isinstance(step.spec["commands"], str):
        command_list = [step.spec["commands"]]
    else:
        command_list = step.spec["commands"]

    job_def_spec = {
        "type": "container",
        "parameters": {
            "repo": "rrr",
            "image": "mmm",
            "inputs": "iii",
            "references": "fff",
            "command": json.dumps(command_list),
            "outputs": "ooo",
            "shell": shell_opt,
            "skip": "sss",
        },
        "containerProperties": {
            "image": os.environ["RUNNER_REPO_URI"] + ":" + os.environ["SOURCE_VERSION"],
            "command": [
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
            "jobRoleArn": task_role,
            **get_environment(),
            **get_resource_requirements(step),
            **get_volume_info(step),
        },
        "schedulingPriority": 1,
        **get_timeout(step),
        "tags": {
            "bclaw:version": os.environ["SOURCE_VERSION"],
        },
    }

    resource_spec = {
        "Type": "Custom::BatchJobDefinition",
        "UpdateReplacePolicy": "Retain",
        "Properties": {
            "ServiceToken": os.environ["JOB_DEF_LAMBDA_ARN"],
            "workflowName": {"Ref": "AWS::StackName"},
            "stepName": step.name,
            "image": expand_image_uri(step.spec["image"]),
            "spec": json.dumps(job_def_spec, sort_keys=True),
        },
    }

    yield Resource(logical_name, resource_spec)
    return logical_name


def get_skip_behavior(spec: dict) -> str:
    if "skip_if_output_exists" in spec and spec["skip_if_output_exists"]:
        ret = "output"
    elif "skip_on_rerun" in spec and spec["skip_on_rerun"]:
        ret = "rerun"
    else:
        ret = "none"

    return ret


def batch_step(step: Step,
               job_definition_logical_name: str,
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
                # this is intended to handle failures caused by Batch API throttling
                #   ... "normal" job failures throw States.TaskFailed
                "ErrorEquals": ["Batch.AWSBatchException"],
                "IntervalSeconds": 30,
                "MaxAttempts": 20,
                "MaxDelaySeconds": 300,
                "BackoffRate": 2.0,
                "JitterStrategy": "FULL",
            },
            {
                "ErrorEquals": ["States.ALL"],
                "IntervalSeconds": time_string_to_seconds(interval),
                "MaxAttempts": attempts,
                "BackoffRate": backoff_rate
            },
        ],
        "Parameters": {
            "JobName.$": job_name,
            "JobDefinition": f"${{{job_definition_logical_name}}}",
            "JobQueue": get_job_queue(step.spec["compute"]),
            "ShareIdentifier.$": "$.share_id",
            "Parameters": {
                "repo.$": "$.repo.uri",
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
                 options: dict,
                 scattered: bool) -> Generator[Resource, None, List[State]]:
    logger = logging.getLogger(__name__)
    logger.info(f"making batch step {step.name}")

    task_role = step.spec.get("task_role") or options.get("task_role") or os.environ["ECS_TASK_ROLE_ARN"]
    shell_opt = step.spec["compute"]["shell"] or options.get("shell")

    job_def_logical_name = yield from job_definition_rc(step, task_role, shell_opt)

    if step.spec["qc_check"] is not None:
        qc_state = handle_qc_check(step)
        ret0 = batch_step(step,
                          job_def_logical_name,
                          **step.spec["retry"],
                          scattered=scattered,
                          next_step_override=qc_state.name)
        ret = [State(step.name, ret0), qc_state]

    else:
        ret = [State(step.name, batch_step(step,
                                           job_def_logical_name,
                                           **step.spec["retry"],
                                           scattered=scattered))]
    return ret
