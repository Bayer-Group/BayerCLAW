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


def handle_qc_check(spec: dict | list | None) -> list:
    if spec is None:
        return []
    if isinstance(spec, dict):
        ret = [spec]
    else:
        ret = spec

    for item in ret:
        if isinstance(item["stop_early_if"], str):
            item.update({"stop_early_if": [item["stop_early_if"]]})

    return ret


def get_consumable_resource_properties(spec: dict) -> dict:
    if spec:
        ret = [{"consumableResource": k, "quantity": v } for k, v in spec.items()]
        return {"consumableResourceProperties": {"consumableResourceList": ret}}
    else:
        return {}


# don't need this here
# def get_repository_credentials(spec: dict) -> dict:
#     if spec["credentials"]:
#         ret = {
#             "repositoryCredentials": {
#                 "credentialsParameter": spec["credentials"]
#             }
#         }
#     else:
#         ret = {}
#
#     return ret


def job_definition_rc(step: Step,
                      task_role: str,
                      shell_opt: str,
                      s3_tags: dict,
                      job_tags: dict) -> Generator[Resource, None, str]:
    logical_name = make_logical_name(f"{step.name}.job.defx")

    # note: this is not a CloudFormation spec, it is meant to be submitted to the Batch API by register.py.
    # To pass it to the registration lambda, it must be json serialized or else the ints and bools will be
    # stringified. Because it gets serialized here in the compiler lambda, though, CloudFormation will not
    # be able to substitute the values for the pseudo parameters (AWS::AccountId, AWS::Region, etc.) in the
    # job definition spec. Therefore, some fields are left unset and will be filled in by register.py.
    job_def_spec = {
        "type": "container",
        "parameters": {
            "repo": "rrr",
            "image": "mmm",
            "inputs": "iii",
            "references": "fff",
            "command": json.dumps(step.spec["commands"], separators=(",", ":")),
            "outputs": "ooo",
            "qc": json.dumps(step.spec["qc_check"], separators=(",", ":")),
            "shell": shell_opt,
            "skip": "sss",
            "s3tags": json.dumps(s3_tags, separators=(",", ":")),
        },
        "containerProperties": {
            "image": os.environ["RUNNER_REPO_URI"] + ":" + os.environ["SOURCE_VERSION"],
            "command": [
                "python", "/bclaw_runner/src/runner_cli.py",
                "-c", "Ref::command",
                "-f", "Ref::references",
                "-i", "Ref::inputs",
                "-k", "Ref::skip",
                "-m", "Ref::image",
                "-o", "Ref::outputs",
                "-q", "Ref::qc",
                "-r", "Ref::repo",
                "-s", "Ref::shell",
                "-t", "Ref::s3tags",
            ],
            "jobRoleArn": task_role,
            **get_environment(),
            **get_resource_requirements(step),
            **get_volume_info(step),
        },
        # oops...wrong place for this
        # **get_repository_credentials(step.spec["image"]),
        **get_consumable_resource_properties(step.spec["compute"]["consumes"]),
        "schedulingPriority": 1,
        **get_timeout(step),
        "propagateTags": True,
        "tags": job_tags | {
            "bclaw:workflow": "",  # placeholder; will be filled in by register.py
            "bclaw:step": step.name,
            "bclaw:version": os.environ["SOURCE_VERSION"],
        },
    }

    # this is a CloudFormation spec. CloudFormation will supply the necessary pseudoparameter
    # values (AWS::StackName, AWS::AccountId, AWS::Region), and register.py will finish the
    # job definition spec and register it with Batch.
    resource_spec = {
        "Type": "Custom::BatchJobDefinition",
        "UpdateReplacePolicy": "Retain",
        "Properties": {
            "ServiceToken": os.environ["JOB_DEF_LAMBDA_ARN"],

            # used to complete the job definition spec and register it
            "workflowName": {"Ref": "AWS::StackName"},

            # the value returned from expand_image_uri may contain AWS::AccountId and AWS::Region,
            # so this needs to be substituted here and passed to register.py. It can be passed unserialized
            # because all the values are strings.
            "image": expand_image_uri(step.spec["image"]),

            # register.py needs the step name to create the job definition name ( <wf_name>_<step_name> ). The
            # alternative is to dig it out of job_def_spec
            "stepName": step.name,
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


def get_output_uris(output_specs: dict) -> dict:
    ret = {}
    for k, v in output_specs.items():
        if "dest" in v:
            ret[k] = f"{v['dest']}{v['name']}"
        else:
            ret[k] = v["name"]
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
                "references": json.dumps(step.spec["references"], separators=(",", ":")),
                "outputs": json.dumps(step.spec["outputs"], separators=(",", ":")),
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
            "Tags": {
                "bclaw:jobfile.$": "$.job_file.key",
            },
        },
        "ResultSelector": {
            **get_output_uris(step.spec["outputs"])
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
    global_and_step_s3_tags = options["s3_tags"] | step.spec["s3_tags"]
    global_and_step_job_tags = options["job_tags"] | step.spec["job_tags"]

    job_def_logical_name = yield from job_definition_rc(step,
                                                        task_role,
                                                        shell_opt,
                                                        global_and_step_s3_tags,
                                                        global_and_step_job_tags)

    ret = [State(step.name, batch_step(step,
                                       job_def_logical_name,
                                       scattered=scattered,
                                       **step.spec["retry"],
                                       ))]
    return ret
