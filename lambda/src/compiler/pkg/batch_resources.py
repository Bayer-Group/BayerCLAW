import json
import logging
import math
import os
import re
from typing import Generator, List, Union

import humanfriendly

from .util import Step, Resource, State, make_logical_name, time_string_to_seconds

SCRATCH_PATH = "/_bclaw_scratch"


def expand_image_uri(image_spec: dict) -> Union[str, dict]:
    uri = image_spec["name"]
    subbed = re.sub(r"\${", "${!", uri)
    # https://stackoverflow.com/questions/37861791/how-are-docker-image-names-parsed
    #   The hostname [of a docker image uri] must contain a . dns separator,
    #   a : port separator, or the value "localhost" before the first /.
    # ...but it's unlikely that localhost will be used in a batch job
    if re.match(r"^.*[.:].*/", subbed):
        ret0 = subbed
    else:
        ret0 = {"Fn::Sub": f"${{AWS::AccountId}}.dkr.ecr.${{AWS::Region}}.amazonaws.com/{subbed}"}

    ret = image_spec.copy()
    ret["name"] = ret0
    return ret


def get_job_queue(compute_spec: dict) -> str:
    gpu_requested = str(compute_spec["gpu"]) != "0"
    if (queue_name := compute_spec.get("queue_name")) is not None:
        ret = f"arn:aws:batch:${{AWSRegion}}:${{AWSAccountId}}:job-queue/{queue_name}"
    elif compute_spec["spot"]:
        ret = os.environ["SPOT_GPU_QUEUE_ARN"] if gpu_requested else os.environ["SPOT_QUEUE_ARN"]
    else:
        ret = os.environ["ON_DEMAND_GPU_QUEUE_ARN"] if gpu_requested else os.environ["ON_DEMAND_QUEUE_ARN"]
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
    ret = {
        "Environment": [
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
            {
                "Name": "AWS_ACCOUNT_ID",
                "Value": {"Ref": "AWS::AccountId"},
            },
        ]
    }
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
                "SourcePath": "/mnt/s3files",
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
            "ReadOnly": False,
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


# def handle_qc_check(spec: dict | list | None) -> list:
#     pass
#     if spec is None:
#         return []
#     if isinstance(spec, dict):
#         ret = [spec]
#     else:
#         ret = spec
#
#     for item in ret:
#         if isinstance(item["stop_early_if"], str):
#             item.update({"stop_early_if": [item["stop_early_if"]]})
#
#     return ret


def get_consumable_resource_properties(spec: dict) -> dict:
    if spec:
        ret = [{"ConsumableResource": k, "Quantity": v } for k, v in spec.items()]
        return {"ConsumableResourceProperties": {"ConsumableResourceList": ret}}
    else:
        return {}


def job_definition_rc(step: Step,
                      task_role: str,
                      shell_opt: str,
                      # s3_tags: dict,
                      # job_tags: dict
                      ) -> Generator[Resource, None, str]:
    logical_name = make_logical_name(f"{step.name}.job.defz")

    job_def = {
        "Type": "AWS::Batch::JobDefinition",
        "UpdateReplacePolicy": "Retain",
        "DeletionPolicy": "Retain",
        "Properties": {
            "JobDefinitionName": {"Fn::Sub": f"${{AWS::StackName}}_{step.name}"},
            "Type": "container",
            "Parameters": {
                "command": json.dumps(step.spec["commands"], separators=(",", ":")),
                "image": json.dumps(expand_image_uri(step.spec["image"]), sort_keys=True, separators=(",", ":")),
                "repo": "rrr",
                "shell": shell_opt,
                "token": "zzz",
                # "inputs": "iii",
                # "references": "fff",
                # "outputs": "ooo",
                # "qc": json.dumps(step.spec["qc_check"], separators=(",", ":")),
                # "skip": "sss",
                # "s3tags": json.dumps(s3_tags, separators=(",", ":")),
            },
            "ContainerProperties": {
                # todo: temp
                "Image": os.environ["RUNNER_REPO_URI"] + ":latest",
                # "Image": os.environ["RUNNER_REPO_URI"] + ":" + os.environ["SOURCE_VERSION"],
                "Command": [
                    "python", "/bclaw_runner/src/runner_cli.py",
                    "-c", "Ref::command",
                    # "-f", "Ref::references",
                    # "-i", "Ref::inputs",
                    # "-k", "Ref::skip",
                    "-m", "Ref::image",
                    # "-o", "Ref::outputs",
                    # "-q", "Ref::qc",
                    "-r", "Ref::repo",
                    "-s", "Ref::shell",
                    # "-t", "Ref::s3tags",
                    "-z", "Ref::token",
                ],
                "JobRoleArn": task_role,
                **get_environment(step),
                **get_resource_requirements(step),
                **get_volume_info(step),
            },
            **get_consumable_resource_properties(step.spec["compute"]["consumes"]),
            "SchedulingPriority": 1,
            **get_timeout(step),
            "ResourceRetentionPolicy": {
                "SkipDeregisterOnUpdate": True,
            },
            "PropagateTags": True,
            # "Tags": job_tags | {
            "Tags": {
                "bclaw:workflow": {"Ref": "AWS::StackName"},
                "bclaw:step": step.name,
                "bclaw:version": os.environ["SOURCE_VERSION"],
            },
        },
    }

    yield Resource(logical_name, job_def)
    return logical_name


# def get_skip_behavior(spec: dict) -> str:
#     if "skip_if_output_exists" in spec and spec["skip_if_output_exists"]:
#         ret = "output"
#     elif "skip_on_rerun" in spec and spec["skip_on_rerun"]:
#         ret = "rerun"
#     else:
#         ret = "none"
#
#     return ret


# def get_output_uris(output_specs: dict) -> dict:
#     ret = {}
#     for k, v in output_specs.items():
#         if "dest" in v:
#             ret[k] = f"{v['dest']}{v['name']}"
#         else:
#             ret[k] = v["name"]
#     return ret


def get_retries(retry_spec: dict, on_error_specs: list) -> Generator[dict, None, None]:
    yield {
        # this is intended to handle failures caused by Batch API throttling
        #   ... "normal" job failures throw States.TaskFailed
        "ErrorEquals": ["Batch.AWSBatchException"],
        "IntervalSeconds": 30,
        "MaxAttempts": 20,
        "MaxDelaySeconds": 300,
        "BackoffRate": 2.0,
        "JitterStrategy": "FULL",
    }

    max_attempts = retry_spec["attempts"]
    backoff_rate = retry_spec["backoff_rate"]
    interval_seconds = time_string_to_seconds(retry_spec["interval"])

    for on_error_spec in on_error_specs:
        if (override := on_error_spec["retries"]) == 0:
            yield {
                "ErrorEquals": [on_error_spec["type"]],
                "MaxAttempts": override,
            }
        else:
            yield {
                "ErrorEquals": [on_error_spec["type"]],
                "MaxAttempts": override,
                "IntervalSeconds": interval_seconds,
                "BackoffRate": backoff_rate,
            }

    yield {
        "ErrorEquals": ["States.ALL"],
        "MaxAttempts": max_attempts,
        "IntervalSeconds": interval_seconds,
        "BackoffRate": backoff_rate,
    }


def get_error_catches(on_error_specs: list) -> Generator[dict, None, None]:
    for spec in on_error_specs:
        if spec["next"]:
            yield {
                "ErrorEquals": [spec["type"]],
                "Next": spec["next"],
                "ResultPath": "$.error_info"
            }


def batch_step(step: Step,
               job_definition_logical_name: str,
               scattered: bool,
               next_step_override: str = None) -> dict:
    # skip_behavior = get_skip_behavior(step.spec)

    if scattered:
        job_name = "States.Format('{}__{}__{}', $$.Execution.Name, $$.State.Name, $.index)"
    else:
        job_name = "States.Format('{}__{}', $$.Execution.Name, $$.State.Name)"

    retries = list(get_retries(step.spec["retry"], step.spec["on_error"]))
    catches = list(get_error_catches(step.spec["on_error"]))

    ret = {
        "Type": "Task",
        "Resource": "arn:aws:states:::batch:submitJob.sync",
        "Retry": retries,
        #     [
        #     {
        #         # this is intended to handle failures caused by Batch API throttling
        #         #   ... "normal" job failures throw States.TaskFailed
        #         "ErrorEquals": ["Batch.AWSBatchException"],
        #         "IntervalSeconds": 30,
        #         "MaxAttempts": 20,
        #         "MaxDelaySeconds": 300,
        #         "BackoffRate": 2.0,
        #         "JitterStrategy": "FULL",
        #     },
        #     {
        #         "ErrorEquals": ["States.ALL"],
        #         "IntervalSeconds": time_string_to_seconds(interval),
        #         "MaxAttempts": attempts,
        #         "BackoffRate": backoff_rate
        #     },
        # ],
        "Parameters": {
            "JobName.$": job_name,
            "JobDefinition": f"${{{job_definition_logical_name}}}",
            "JobQueue": get_job_queue(step.spec["compute"]),
            "ShareIdentifier.$": "$.share_id",
            "Parameters": {
                "repo.$": "$.repo",
                "token.$": "$$.Task.Token",
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
            "Tags": {
                "bclaw:jobfile.$": "$.job_file.key",
            },
        },
        # "ResultSelector": {
        #     "status.$": "$.Status",
        # },
        # "ResultPath": "$.result",
        "ResultPath": None,
        "OutputPath": "$",
    }

    if catches:
        ret["Catch"] = catches

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
    # global_and_step_s3_tags = options["s3_tags"] | step.spec["s3_tags"]
    # global_and_step_job_tags = options["job_tags"] | step.spec["job_tags"]

    job_def_logical_name = yield from job_definition_rc(step,
                                                        task_role,
                                                        shell_opt)
                                                        # global_and_step_s3_tags,
                                                        # global_and_step_job_tags)

    ret = [State(step.name, batch_step(step,
                                       job_def_logical_name,
                                       scattered=scattered  #,
                                       # **step.spec["retry"],
                                       ))]
    return ret
