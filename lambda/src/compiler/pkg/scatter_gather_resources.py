import json
import logging
import os
import re
from typing import Generator, List

from . import state_machine_resources as sm
from .util import Step, Resource, State, lambda_logging_block, lambda_retry


def scatter_step(step: Step, map_step_name: str) -> dict:
    ret = {
        "Type": "Task",
        "Resource": os.environ["SCATTER_LAMBDA_ARN"],
        "Parameters": {
            "repo.$": "$.repo",
            "scatter": json.dumps(step.spec["scatter"]),
            **step.input_field,
            **lambda_logging_block(step.name),
        },
        **lambda_retry(),
        "ResultPath": "$.scatter",
        "Next": map_step_name
    }

    return ret


# def map_step0(sub_branch: dict, gather_step_name: str) -> dict:
#     ret = {
#         "Type": "Map",
#         "ItemsPath": "$.items",
#         "Parameters": {
#             "index.$": "States.Format('{}', $$.Map.Item.Index)",  # stringify the index
#             "job_file.$": "$.job_file",
#             "prev_outputs": {},
#             "repo.$": "$$.Map.Item.Value.repo",
#             "share_id.$": "$.share_id",
#         },
#         "Iterator": sub_branch,
#         "ResultPath": None,
#         "Next": gather_step_name,
#     }
#
#     return ret


def error_tolerance(spec: dict) -> dict:
    # spec["error_tolerance"]["count"] = 0 by default (set by validator)
    if "percent" in spec["error_tolerance"]:
        ret = {"ToleratedFailurePercentage": spec["error_tolerance"]["percent"]}
    else:
        ret = {"ToleratedFailureCount": spec["error_tolerance"]["count"]}
    return ret


def map_step(step: Step, sub_branch: dict, gather_step_name: str) -> dict:
    label = re.sub(r"\W", "", step.name)

    ret = {
        "Type": "Map",
        "MaxConcurrency": step.spec["max_concurrency"],
        **error_tolerance(step.spec),
        "Label": label[:40],
        "ItemReader": {
            "Resource": "arn:aws:states:::s3:getObject",
            "ReaderConfig": {
                "InputType": "CSV",
                "CSVHeaderLocation": "FIRST_ROW",
            },
            "Parameters": {
                "Bucket.$": "$.scatter.items.bucket",
                "Key.$": "$.scatter.items.key",
            }
        },
        "ItemSelector": {
            "index.$": "States.Format('{}', $$.Map.Item.Index)",  # stringify the index
            "job_file.$": "$.job_file",
            "prev_outputs": {},
            "scatter.$": "$$.Map.Item.Value",
            "repo.$": "$.scatter.repo",
            "share_id.$": "$.share_id"
        },
        "ItemProcessor": {
            "ProcessorConfig": {
                "Mode": "DISTRIBUTED",
                "ExecutionType": "STANDARD"
            },
            **sub_branch
        },
        # "ResultWriter": {
        #     "Resource": "arn:aws:states:::s3:putObject",
        #     "Parameters": {
        #         "Bucket.$": "$.repo.bucket",  # ergh
        #         "Prefix.$": "$.repo.prefix",  # double ergh
        #     }
        # },
        "ResultPath": None,
        "Next": gather_step_name,
    }

    return ret


def scatter_init_step(parent_step_name: str) ->  dict:
    step_name = f"{parent_step_name}.initialize"
    ret = {
        step_name: {
            "Type": "Task",
            "Resource": os.environ["SCATTER_INIT_LAMBDA_ARN"],
            "Parameters": {
                "index.$": "$.index",
                "repo.$": "$.repo",
                "scatter.$": "$.scatter",
                **lambda_logging_block(step_name)
            },
            **lambda_retry(max_attempts=10),
            "ResultPath": "$.repo",
            "_stet": True,
        },
    }
    return ret


def gather_step(step: Step) -> dict:
    ret = {
        "Type": "Task",
        "Resource": os.environ["GATHER_LAMBDA_ARN"],
        "Parameters": {
            "repo.$": "$.repo",
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

    sub_branch = yield from sm.make_branch([scatter_init_step(step.name)] + step.spec["steps"],
                                           options, depth=map_depth + 1)

    scatter_step_name = step.name
    map_step_name = f"{step.name}.map"
    gather_step_name = f"{step.name}.gather"

    ret = [
        State(scatter_step_name, scatter_step(step, map_step_name)),
        State(map_step_name, map_step(step, sub_branch, gather_step_name)),
        State(gather_step_name, gather_step(step))
    ]

    return ret
