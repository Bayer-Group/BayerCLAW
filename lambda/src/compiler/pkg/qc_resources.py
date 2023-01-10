import os
from .util import Step, State, lambda_logging_block


def qc_checker_step(batch_step: Step,
                    retry_attempts: int = 3,
                    wait_interval: int = 3,
                    backoff_rate: float = 1.5) -> dict:
    qc_spec = batch_step.spec["qc_check"]

    ret = {
        "Type": "Task",
        "Resource": os.environ["QC_CHECKER_LAMBDA_ARN"],  # core_stack.output("QCCheckerLambdaArn"),
        "Parameters": {
            "repo.$": "$.repo",
            "qc_result_file": qc_spec["qc_result_file"],
            "qc_expression": qc_spec["stop_early_if"],
            "execution_id.$": "$$.Execution.Id",
            **lambda_logging_block(batch_step.name),
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
        **batch_step.next_or_end,
    }

    return ret


def handle_qc_check(batch_step: Step) -> State:
    qc_checker_step_name = f"{batch_step.name}.qc_checker"
    ret = State(qc_checker_step_name, qc_checker_step(batch_step))
    return ret
