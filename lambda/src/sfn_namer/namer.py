import json
import logging
import os
import re

import boto3

from lambda_logs import JSONFormatter, custom_lambda_logs

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.handlers[0].setFormatter(JSONFormatter())


def shorten_filename(key: str) -> str:
    # assume the filename extension is something uninformative like ".json"
    root = os.path.splitext(key)[0]
    # top level dir is constant for a workflow so discard it
    ret = re.split(r"/+", root, maxsplit=1)[-1]
    return ret


def normalize(string: str) -> str:
    ret0 = re.sub(r"[^A-Za-z0-9]+", "-", string)
    ret = ret0.strip("-")
    return ret


def make_execution_name(s3_key: str, version: str, replay: str) -> str:
    norm_key = normalize(shorten_filename(s3_key))
    norm_version = normalize(version)
    if replay == "":
        ret = f"{norm_key:.71}_{norm_version:.8}"
    else:
        norm_replay = normalize(replay)
        ret = f"{norm_replay:.10}_{norm_key:.60}_{norm_version:.8}"
    return ret


def lambda_handler(event: dict, context: object) -> None:
    # event = {
    #   "branch": "..."
    #   "job_file_bucket": "...",
    #   "job_file_key": "...",
    #   "job_file_version": "..."  # empty string if launcher bucket versioning is suspended
    #   "workflow_name": "..."
    #   "replay": "...",  # empty string if not an archive replay
    #   "sfn_arn": "..."
    # }

    with custom_lambda_logs(**event):
        logger.info(f"{event=}")
        sfn = boto3.client("stepfunctions")

        try:
            # todo: remove
            assert "_DIE_DIE_DIE_" not in event["job_file_key"]

            exec_name = make_execution_name(event["job_file_key"],
                                            event["job_file_version"],
                                            event["replay"])
            logger.info(f"{exec_name=}")

            input_obj = {
                "job_file": {
                    "bucket": event["job_file_bucket"],
                    "key": event["job_file_key"],
                    "version": event["job_file_version"],
                    "s3_request_id": "DEPRECATED",
                },
                "index": event["branch"],
            }

            response = sfn.start_execution(
                stateMachineArn=event["sfn_arn"],
                name=exec_name,
                input=json.dumps(input_obj)
            )
            logger.info(f"{response=}")

        except sfn.exceptions.ExecutionAlreadyExists:
            # duplicated s3 events are way more likely than bona fide name collisions
            logger.info(f"duplicate event: {exec_name}")
