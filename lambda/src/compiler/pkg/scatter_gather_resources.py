import json
import logging
import os
from typing import Generator, List

from . import state_machine_resources as sm
from .util import Step, Resource, State, lambda_logging_block, lambda_retry


def scatter_step(step: Step, map_step_name: str) -> dict:
    ret = {
        "Type": "Task",
        "Resource": os.environ["SCATTER_LAMBDA_ARN"],
        "Parameters": {
            "repo.$": "$.repo.uri",
            "scatter": json.dumps(step.spec["scatter"]),
            **step.input_field,
            **lambda_logging_block(step.name),
        },
        **lambda_retry(),
        "ResultPath": "$.items",
        "Next": map_step_name
    }

    return ret


def map_step(sub_branch: dict, gather_step_name: str) -> dict:
    ret = {
        "Type": "Map",
        "ItemsPath": "$.items",
        "Parameters": {
            "index.$": "States.Format('{}', $$.Map.Item.Index)",  # stringify the index
            "job_file.$": "$.job_file",
            "prev_outputs": {},
            "repo.$": "$$.Map.Item.Value.repo",
            "share_id.$": "$.share_id",
        },
        "Iterator": sub_branch,
        "ResultPath": None,
        "Next": gather_step_name,
    }

    return ret


def map_step1(sub_branch: dict, gather_step_name: str) -> dict:
    step_name = "needStepName"

    ret = {
        "Type": "Task",
        "ItemReader": {
            "ReaderConfig": {
                "InputType": "JSON"
            },
            "Resource": "arn:aws:states:::s3:getObject",
            "Parameters": {
                "Bucket.$": "$.repo.bucket",
                "Key.$": f"States.Format('{{}}/{step_name}/items.json', $.repo.prefix)",
            }
        },
        # "ItemsPath": {},
        "ItemSelector": {
            "index.$": "States.Format('{}', $$.Map.Item.Index)",  # stringify the index
            "job_file.$": "$.job_file",
            "prev_outputs": {},
            "repo.$": "$$.Map.Item.Value",
            "share_id.$": "$.share_id"
        },
        # ItemBatcher?
        # MaxConcurrency*?
        # ToleratedFailure*?
        "Label": step_name,
        "ItemProcessor": {
            "ProcessorConfig": {
                "Mode": "DISTRIBUTED",
                "ExecutionType": "STANDARD"
            },
            **sub_branch
        },
        "ResultWriter": {
            "Resource": "arn:aws:states:::s3:putObject",
            "Parameters": {
                "Bucket.$": "$.repo.bucket",
                "Prefix.$": "$.repo.prefix",
            }
        },
        "ResultPath": None,
        "Next": gather_step_name,
    }

    return ret

def gather_step(step: Step) -> dict:
    ret = {
        "Type": "Task",
        "Resource": os.environ["GATHER_LAMBDA_ARN"],
        "Parameters": {
            "repo.$": "$.repo.uri",
            "outputs": json.dumps(step.spec["outputs"]),
            "items.$": "$.items",
            **lambda_logging_block(step.name),
        },
        **lambda_retry(),
        "ResultPath": "$.prev_outputs",
        "OutputPath": "$",
        **step.next_or_end,
    }

    return ret


def handle_scatter_gather(step: Step,
                          options: dict,
                          map_depth: int
                          ) -> Generator[Resource, None, List[State]]:
    logger = logging.getLogger(__name__)
    logger.info(f"making scatter gather steps for {step.name}")

    if map_depth > 0:
        raise RuntimeError("Nested Scatter steps are not supported")

    sub_branch = yield from sm.make_branch(step.spec["steps"], options, depth=map_depth + 1)

    scatter_step_name = step.name
    map_step_name = f"{step.name}.map"
    gather_step_name = f"{step.name}.gather"

    ret = [
        State(scatter_step_name, scatter_step(step, map_step_name)),
        State(map_step_name, map_step(sub_branch, gather_step_name)),
        State(gather_step_name, gather_step(step))
    ]

    return ret
