import json
import logging
import os
import re

import boto3

from lambda_logs import JSONFormatter, custom_lambda_logs

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.handlers[0].setFormatter(JSONFormatter())


def normalize(key: str) -> str:
    # assume the filename extension is something uninformative like ".json"
    root0 = os.path.splitext(key)[0]
    # top level dir is constant for a workflow so discard it
    root = re.split(r"/+", root0, maxsplit=1)[-1]
    ret = re.sub(r"[^A-Za-z0-9]+", "-", root)
    return ret


def make_execution_name(s3_key: str, version: str, replay: str) -> str:
    norm_key = normalize(s3_key)
    if replay == "":
        ret = f"{norm_key:.71}_{version:.8}"
    else:
        ret = f"{replay:.10}_{norm_key:.60}_{version:.8}"
    return ret


def lambda_handler(event: dict, context: object) -> None:
    # event = {
    #   "job_file": {
    #     "bucket": "...",
    #     "key": "...",
    #     "version": "..."
    #   },
    #   "replay": "...",  # empty string if not an archive replay
    #   "index": "main",
    #   "sfn_arn": "..."
    # }

    # with custom_lambda_logs(branch=event["index"],
    #                         job_file_bucket=event["job_file"]["bucket"],
    #                         job_file_key=event["job_file"]["key"],
    #                         job_file_version=event["job_file"]["version"],
    #                         job_file_s3_request_id="DEPRECATED",
    #                         sfn_execution_id="unassigned",
    #                         step_name="pre-launch",
    #                         workflow_name="todo"):
    with custom_lambda_logs(**event):
        logger.info(f"{event = }")
        sfn = boto3.client("stepfunctions")

        try:
            # todo: remove
            assert "_DIE_DIE_DIE_" not in event["job_file_key"]

            # state_machine_arn = event["sfn_arn"]
            # replay = event["replay"]
            exec_name = make_execution_name(event["job_file_key"],
                                            event["job_file_version"],
                                            event["replay"])

            logger.info(f"{exec_name =}")

            input_obj = {
                "job_file": {
                    "bucket": event["job_file_bucket"],
                    "key": event["job_file_key"],
                    "version": event["job_file_version"],
                    "s3_request_id": "DEPRECATED",
                },
                "index": event["branch"],
            }

            while True:
                response = sfn.start_execution(
                    stateMachineArn=event["sfn_arn"],
                    name=exec_name,
                    input=json.dumps(input_obj)
                    # input=json.dumps(event)
                )

                logger.info(f"{response = }")

                if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                    break

        except sfn.exceptions.ExecutionAlreadyExists:
            # duplicated s3 events are way more likely than bona fide name collisions
            logger.info(f"duplicate event: {exec_name}")

        except Exception:
            logger.exception("failed: ")
