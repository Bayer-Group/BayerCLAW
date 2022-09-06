import json
import logging
from typing import List

from .util import CoreStack, Step, State, lambda_logging_block, lambda_retry


def file_submit_step(core_stack: CoreStack, step: Step, run_subpipe_step_name: str) -> dict:
    ret = {
        "Type": "Task",
        "Resource": core_stack.output("SubpipesLambdaArn"),
        "Parameters": {
            "repo.$": "$.repo",
            "submit": json.dumps(step.spec["submit"]),
            **lambda_logging_block(step.name),
        },
        **lambda_retry(),
        "ResultPath": "$.subpipe",
        "OutputPath": "$",
        "Next": run_subpipe_step_name,
    }

    return ret


def run_subpipe_step(step: Step, retrieve_step_name: str) -> dict:
    state_machine_arn = step.spec["subpipe"]

    if not state_machine_arn.startswith("arn:"):
        state_machine_arn = "arn:aws:states:${AWSRegion}:${AWSAccountId}:stateMachine:" + state_machine_arn

    ret = {
        "Type": "Task",
        "Resource": "arn:aws:states:::states:startExecution.sync",
        "Parameters": {
            "Input": {
                "index": "main",
                "job_file.$": "$.job_file",
                "prev_outputs": {},
                "repo.$": "$.subpipe.sub_repo",
                "AWS_STEP_FUNCTIONS_STARTED_BY_EXECUTION_ID.$": "$$.Execution.Id",
            },
            "StateMachineArn": state_machine_arn,
        },
        "ResultPath": None,
        "OutputPath": "$",
        "Next": retrieve_step_name
    }

    return ret


def file_retrieve_step(core_stack: CoreStack, step: Step) -> dict:
    ret = {
        "Type": "Task",
        "Resource": core_stack.output("SubpipesLambdaArn"),
        "Parameters": {
            "repo.$": "$.repo",
            "retrieve": json.dumps(step.spec["retrieve"]),
            "subpipe": {
                "sub_repo.$": "$.subpipe.sub_repo",
            },
            **lambda_logging_block(step.name)
        },
        **lambda_retry(),
        "ResultSelector": {},
        "ResultPath": "$.prev_outputs",
        "OutputPath": "$",
        **step.next_or_end,
    }

    return ret


def handle_subpipe(core_stack: CoreStack,
                   step: Step
                   ) -> List[State]:
    logger = logging.getLogger(__name__)
    logger.info(f"making subpipe step {step.name}")

    submit_step_name = step.name
    subpipe_step_name = f"{step.name}.subpipe"
    retrieve_step_name = f"{step.name}.retrieve"

    ret = [
        State(submit_step_name, file_submit_step(core_stack, step, subpipe_step_name)),
        State(subpipe_step_name, run_subpipe_step(step, retrieve_step_name)),
        State(retrieve_step_name, file_retrieve_step(core_stack, step)),
    ]

    return ret
