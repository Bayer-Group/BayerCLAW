from datetime import timedelta
import json
import os
import re
from typing import Any, NamedTuple

import boto3
import jmespath


class Step(NamedTuple):
    name: str
    spec: dict
    next: str

    @property
    def is_terminal(self) -> bool:
        return self.next == ""

    @property
    def next_or_end(self) -> dict:
        if self.is_terminal:
            return {"End": True}
        else:
            return {"Next": self.next}

    @property
    def input_field(self) -> dict:
        if self.spec["inputs"] is None:
            ret = {"inputs.$": "States.JsonToString($.prev_outputs)"}
        else:
            ret = {"inputs": json.dumps(self.spec["inputs"])}
        return ret


class Resource(NamedTuple):
    name: str
    spec: dict


class State(NamedTuple):
    name: str
    spec: dict


def make_logical_name(s: str) -> str:
    words = (w.capitalize() for w in re.split(r"[\W_]+", s))
    ret = "".join(words)
    return ret


# given "${something}":
#   match.group(0) == "${something}"
#   match.group(1) == "something"
PARAM_FINDER = re.compile(r"\${([A-Za-z0-9]+)}")

def substitute_params(params: dict, target: Any):
    if isinstance(target, str):
        ret = PARAM_FINDER.sub(lambda m: str(params.get(m.group(1), m.group(0))), target)
    elif isinstance(target, list):
        ret = [substitute_params(params, v) for v in target]
    elif isinstance(target, dict):
        ret = {k: substitute_params(params, v) for k, v in target.items()}
    else:
        ret = target
    return ret


def lambda_logging_block(step_name: str) -> dict:
    ret = {
        "logging": {
            "branch.$": "$.index",
            "job_file_bucket.$": "$.job_file.bucket",
            "job_file_key.$": "$.job_file.key",
            "job_file_version.$": "$.job_file.version",
            "sfn_execution_id.$": "$$.Execution.Name",
            "step_name": step_name,
            "workflow_name": "${WorkflowName}",
        },
    }
    return ret


def time_string_to_seconds(time: str) -> int:
    units = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days", "w": "weeks"}
    count = int(time[:-1])
    unit = units[time[-1]]
    td = timedelta(**{unit: count})
    ret = td.seconds + 60 * 60 * 24 * td.days
    return ret


def lambda_retry() -> dict:
    # https://docs.aws.amazon.com/step-functions/latest/dg/bp-lambda-serviceexception.html
    ret = {
        "Retry": [
            {
                "ErrorEquals": [
                    "Lambda.ServiceException",
                    "Lambda.AWSLambdaException",
                    "Lambda.SdkClientException",
                    "Lambda.TooManyRequestsException",
                ],
                "MaxAttempts": 5,
                "IntervalSeconds": 2,
                "BackoffRate": 2,
                "JitterStrategy": "FULL",
            },
        ]
    }
    return ret


def merge_params_and_options(params: dict, options: dict) -> dict:
    ret = params | options
    if ret["task_role"] is None:
        ret["task_role"] = params["task_role"]
    return ret
