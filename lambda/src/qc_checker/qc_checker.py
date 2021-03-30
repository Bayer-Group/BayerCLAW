from contextlib import closing
import json
import logging

import boto3

from lambda_logs import JSONFormatter, custom_lambda_logs

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.handlers[0].setFormatter(JSONFormatter())


class QCFailed(Exception):
    def __init__(self, message: str):
        self.message = message


def lambda_handler(event: dict, context: object):
    with custom_lambda_logs(**event["logging"]):
        logger.info(f"event: {str(event)}")

        s3_path = f"{event['repo']}/{event['qc_result_file']}"
        bucket, key = s3_path.split("/", 3)[2:]

        s3 = boto3.client("s3")
        response = s3.get_object(Bucket=bucket, Key=key)
        with closing(response["Body"]) as fp:
            qc_object = json.load(fp)

        logger.info(f"input: {str(qc_object)}")

        result = eval(event["qc_expression"], globals(), qc_object)

        if result:
            logger.warning("failed QC check")
            sfn = boto3.client("stepfunctions")
            sfn.stop_execution(
                executionArn=event["execution_id"],
                # todo: need to look in the logging block?
                error=f"Job {event['logging']['job_file_key']} failed QC check at step {event['logging']['step_name']}",
                cause=f"failed condition: {event['qc_expression']}"
            )
            raise QCFailed(f"QC check failed ({event['qc_expression']})")
        else:
            logger.info("passed QC check")
