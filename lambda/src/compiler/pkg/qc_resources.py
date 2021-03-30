from typing import Tuple, List

from .util import CoreStack, Step, State, next_or_end, lambda_logging_block


def qc_checker_step(core_stack: CoreStack, step_name: str, qc_spec: dict, next_step: Step,
                    retry_attempts: int = 3, wait_interval: int = 3, backoff_rate: float = 1.5) -> dict:
    ret = {
        "Type": "Task",
        "Resource": core_stack.output("QCCheckerLambdaArn"),
        "Parameters": {
            "repo.$": "$.repo",
            "qc_result_file": qc_spec["qc_result_file"],
            "qc_expression": qc_spec["stop_early_if"],
            "execution_id.$": "$$.Execution.Id",
            **lambda_logging_block(step_name),
        },
        "Retry": [
            {
                "ErrorEquals": ["QCFailed"],
                # If the execution fails QC, the checker lambda will abort the pipeline.
                # It should happen quickly, adding a long delay in case it doesn't.
                "IntervalSeconds": 3600,
                "MaxAttempts": 1,
            },
            {
                "ErrorEquals": ["States.ALL"],
                "IntervalSeconds": wait_interval,
                "MaxAttempts": retry_attempts,
                "BackoffRate": backoff_rate,
            }
        ],
        "ResultPath": None,
        "OutputPath": "$",
        **next_or_end(next_step)
    }

    return ret


def handle_qc_check(core_stack: CoreStack, step_name: str, qc_spec: dict, next_step: Step) -> Tuple[str, List[State]]:
    qc_checker_step_name = f"{step_name}.qc_checker"

    ret = [
        State(qc_checker_step_name, qc_checker_step(core_stack, step_name, qc_spec, next_step)),
    ]

    return qc_checker_step_name, ret
