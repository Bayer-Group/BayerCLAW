import json
import logging
import os
from typing import Generator

import boto3


logger = logging.getLogger(__name__)

class QCFailure(Exception):
    def __init__(self, message: str, failures: list):
        super().__init__(message)
        self.failures = failures


def abort_execution(failed_expressions: list) -> None:
    logger.warning("aborting workflow execution")

    region = os.environ["AWS_DEFAULT_REGION"]
    acct = os.environ["AWS_ACCOUNT_ID"]
    wf_name = os.environ["BC_WORKFLOW_NAME"]
    exec_id = os.environ["BC_EXECUTION_ID"]
    step_name = os.environ["BC_STEP_NAME"]
    execution_arn = f"arn:aws:states:{region}:{acct}:execution:{wf_name}:{exec_id}"

    cause = "failed QC conditions: " + "; ".join(failed_expressions)

    sfn = boto3.client("stepfunctions")
    sfn.stop_execution(
        executionArn=execution_arn,
        error=f"Job {exec_id} failed QC check at step {step_name}",
        cause=cause
    )


def run_one_qc_check(qc_data: dict, qc_expression: str) -> bool:
    if result := eval(qc_expression, globals(), qc_data):
        logger.warning(f"failed QC check: {qc_expression}")
    else:
        logger.info(f"passed QC check: {qc_expression}")
    return result


def run_all_qc_checks(checks: list) -> Generator[str, None, None]:
    for item in checks:
        qc_file = item["qc_result_file"]
        logger.info(f"{qc_file=}")

        with open(qc_file) as fp:
            qc_data = json.load(fp)

        for qc_expression in item["stop_early_if"]:
            if run_one_qc_check(qc_data, qc_expression):
                yld = f"{os.path.basename(qc_file)}: {qc_expression}"
                yield yld


def do_checks(checks: list) -> None:
    if checks:
        logger.info("starting QC checks")
        if (failures := list(run_all_qc_checks(checks))):
            raise QCFailure("QC checks failed", failures)
        logger.info("QC checks finished")
    else:
        logger.info("no QC checks requested")
