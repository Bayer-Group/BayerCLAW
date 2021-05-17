from datetime import timedelta
import os
import re
from typing import Dict, Union, Any, NamedTuple

import boto3
import jmespath


class Step(NamedTuple):
    name: str
    spec: dict

    @property
    def next_or_end(self) -> dict:
        ret = {k: self.spec[k] for k in {"Next", "End"} & set(self.spec)}
        return ret


class Resource(NamedTuple):
    name: str
    spec: dict


class State(NamedTuple):
    name: str
    spec: dict


SENTRY = Step("SENTRY", {})


class CoreStack(object):
    def __init__(self):
        self.name = os.environ["CORE_STACK_NAME"]

        cfn = boto3.resource("cloudformation")
        self.core_stack = cfn.Stack(self.name)

    def output(self, output_key: str) -> str:
        try:
            query = f"[?OutputKey=='{output_key}'].OutputValue"
            ret = jmespath.search(query, self.core_stack.outputs)[0]
        except IndexError:
            raise RuntimeError(f"{output_key} not found in core stack outputs")
        return ret


def make_logical_name(s: str) -> str:
    words = (w.capitalize() for w in re.split(r"[\W_]+", s))
    ret = "".join(words)
    return ret


# def next_or_end2(step: Step) -> dict:
#     ret = {k: step.spec[k] for k in {"Next", "End"} & set(step.spec)}
#     return ret


def next_or_end(next_step: Step) -> Dict[str, Union[bool, str]]:
    if next_step is SENTRY:
        ret = {"End": True}
    else:
        ret = {"Next": next_step.name}

    return ret


# given "${something}":
#   match.group(0) == "${something}"
#   match.group(1) == "something"
PARAM_FINDER = re.compile(r"\${([\w.]+)}")

def _param_subber(params: dict, target: Any):
    if isinstance(target, str):
        ret = PARAM_FINDER.sub(lambda m: str(params.get(m.group(1), m.group(0))), target)
    elif isinstance(target, list):
        ret = [_param_subber(params, v) for v in target]
    elif isinstance(target, dict):
        ret = {k: _param_subber(params, v) for k, v in target.items()}
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
            "job_file_s3_request_id.$": "$.job_file.s3_request_id",
            "sfn_execution_id.$": "$$.Execution.Name",
            "step_name": step_name,
            "workflow_name": "${WorkflowName}",
        },
    }
    return ret


def do_param_substitution(spec: dict) -> dict:
    ret = {}

    for k, v in spec.items():
        if k in {"inputs", "commands", "outputs"}:
            ret[k] = _param_subber(spec["params"], v)
        elif k == "steps":
            parent_params = {f"parent.{k}": v for k, v in spec["params"].items()}
            ret[k] = _param_subber(parent_params, v)
        elif k == "params":
            ret[k] = {}
        else:
            ret[k] = v

    return ret


def time_string_to_seconds(time: str) -> int:
    UNITS = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days", "w": "weeks"}
    count = int(time[:-1])
    unit = UNITS[time[-1]]
    td = timedelta(**{unit: count})
    ret = td.seconds + 60 * 60 * 24 * td.days
    return ret
