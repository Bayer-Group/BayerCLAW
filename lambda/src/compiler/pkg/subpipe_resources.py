import json
import logging
from typing import List

from .util import CoreStack, Step, State, next_or_end, lambda_logging_block


def file_submit_step(core_stack: CoreStack, step_name: str, spec: dict, next_step_name: str) -> dict:
    ret = {
        "Type": "Task",
        "Resource": core_stack.output("SubpipesLambdaArn"),
        "Parameters": {
            "repo.$": "$.repo",
            "submit": json.dumps(spec["submit"]),
            **lambda_logging_block(step_name),
        },
        "ResultPath": "$.subpipe",
        "OutputPath": "$",
        "Next": next_step_name,
    }

    return ret


def run_subpipe_step(spec: dict, next_step_name: str) -> dict:
    if spec["subpipe"].startswith("arn:"):
        state_machine_arn = spec["subpipe"]
    else:
        # todo: test
        state_machine_arn = "arn:aws:states:${AWSRegion}:${AWSAccountId}:stateMachine:" + spec['subpipe']

    ret = {
        "Type": "Task",
        "Resource": "arn:aws:states:::states:startExecution.sync",
        "Parameters": {
            "Input": {
                "index": "main",
                "id_prefix.$": "$.id_prefix",
                "job_file.$": "$.job_file",
                "repo.$": "$.subpipe.sub_repo",
                "AWS_STEP_FUNCTIONS_STARTED_BY_EXECUTION_ID.$": "$$.Execution.Id",
            },
            "StateMachineArn": state_machine_arn,
        },
        "ResultPath": None,
        "OutputPath": "$",
        "Next": next_step_name
    }

    return ret


def file_retrieve_step(core_stack: CoreStack, step_name: str, spec: dict, next_step: Step) -> dict:
    ret = {
        "Type": "Task",
        "Resource": core_stack.output("SubpipesLambdaArn"),
        "Parameters": {
            "repo.$": "$.repo",
            "retrieve": json.dumps(spec["retrieve"]),
            "subpipe": {
                "sub_repo.$": "$.subpipe.sub_repo",
            },
            **lambda_logging_block(step_name)
        },
        "ResultPath": None,
        "OutputPath": "$",
        **next_or_end(next_step)
    }

    return ret


def handle_subpipe(core_stack: CoreStack, step_name: str, spec: dict, next_step: Step) -> List[State]:
    logger = logging.getLogger(__name__)
    logger.info(f"making subpipe step {step_name}")

    submit_step_name = step_name
    subpipe_step_name = f"{step_name}.subpipe"
    retrieve_step_name = f"{step_name}.retrieve"

    ret = [
        State(submit_step_name, file_submit_step(core_stack, step_name, spec, subpipe_step_name)),
        State(subpipe_step_name, run_subpipe_step(spec, retrieve_step_name)),
        State(retrieve_step_name, file_retrieve_step(core_stack, step_name, spec, next_step)),
    ]

    return ret
