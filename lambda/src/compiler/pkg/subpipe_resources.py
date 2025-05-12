import json
import logging
import os
from typing import List

from .util import Step, State, lambda_logging_block, lambda_retry


def file_submit_step(step: Step, run_subpipe_step_name: str) -> dict:
    ret = {
        "Type": "Task",
        "Resource": os.environ["SUBPIPES_LAMBDA_ARN"],
        "Parameters": {
            "repo.$": "$.repo.uri",
            "job_data": step.spec["job_data"],
            "submit": json.dumps(step.spec["submit"]),
            "step_name": step.name,
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
                "share_id.$": "$.share_id",
                "AWS_STEP_FUNCTIONS_STARTED_BY_EXECUTION_ID.$": "$$.Execution.Id",
            },
            # todo: this could get to be too long if you have nested subpipes
            #   might be better to compute it in subpipe lambda
            "Name.$": f"States.Format('{{}}_{step.name}', $$.Execution.Name)",
            "StateMachineArn": state_machine_arn,
        },
        "ResultPath": None,
        "OutputPath": "$",
        "Next": retrieve_step_name
    }

    return ret


def file_retrieve_step(step: Step) -> dict:
    ret = {
        "Type": "Task",
        "Resource": os.environ["SUBPIPES_LAMBDA_ARN"],
        "Parameters": {
            "repo.$": "$.repo.uri",
            "retrieve": json.dumps(step.spec["retrieve"]),
            "subpipe": {
                "sub_repo.$": "$.subpipe.sub_repo.uri",
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


def handle_subpipe(step: Step) -> List[State]:
    logger = logging.getLogger(__name__)
    logger.info(f"making subpipe step {step.name}")

    submit_step_name = step.name
    subpipe_step_name = f"{step.name}.subpipe"
    retrieve_step_name = f"{step.name}.retrieve"

    ret = [
        State(submit_step_name, file_submit_step(step, subpipe_step_name)),
        State(subpipe_step_name, run_subpipe_step(step, retrieve_step_name)),
        State(retrieve_step_name, file_retrieve_step(step)),
    ]

    return ret
