import json
import logging
import os

import boto3

logger = logging.getLogger(__name__)


def run_qc_checks(checks: list) -> None:
    logger.info("starting QC checks")
    for item in checks:
        qc_file = item["qc_result_file"]
        logger.info(f"{qc_file=}")

        with open(qc_file) as fp:
            qc_data = json.load(fp)

        for qc_expression in item["stop_early_if"]:
            run_qc_check(qc_data, qc_expression)

    logger.info("QC checks finished")


def run_qc_check(qc_data: dict, qc_expression: str) -> None:
    result = eval(qc_expression, globals(), qc_data)
    if result:
        logger.warning(f"failed QC check: {qc_expression}; aborting")
        abort_execution(qc_expression)
    else:
        logger.info(f"passed QC check: {qc_expression}")


def abort_execution(qc_expression: str) -> None:
    region = os.environ["AWS_DEFAULT_REGION"]
    acct = os.environ["AWS_ACCOUNT_ID"]
    wf_name = os.environ["BC_WORKFLOW_NAME"]
    exec_id = os.environ["BC_EXECUTION_ID"]
    step_name = os.environ["BC_STEP_NAME"]
    execution_arn = f"arn:aws:states:{region}:{acct}:execution:{wf_name}:{exec_id}"

    sfn = boto3.client("stepfunctions")
    sfn.stop_execution(
        executionArn=execution_arn,
        error=f"Job {exec_id} failed QC check at step {step_name}",
        cause=f"failed condition: {qc_expression}"
    )
