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
    ret = os.path.splitext(key)[0]
    # top level dir is constant for a workflow so discard it
    # ret = re.split(r"/+", root, maxsplit=1)[-1]
    return ret


def normalize(string: str) -> str:
    ret0 = re.sub(r"[^A-Za-z0-9]+", "-", string)
    ret = ret0.strip("-")
    return ret


def make_execution_name(s3_path: str, version: str) -> str:
    # assumes the top level directory (= workflow name) has been stripped from s3_path
    norm_key = normalize(shorten_filename(s3_path))
    norm_version = normalize(version) or "NULL"
    ret = f"{norm_key:.71}_{norm_version:.8}"
    return ret


def lambda_handler(event: dict, context: object) -> None:
    # event = {
    #   branch: str
    #   job_file_bucket: str,
    #   job_file_key: str,
    #   job_file_version: str  # empty string if launcher bucket versioning is suspended
    # }

    with custom_lambda_logs(**event):
        logger.info(f"{event=}")

        assert "_DIE_DIE_DIE_" not in event["job_file_key"]

        sfn = boto3.client("stepfunctions")

        try:
            # todo: split this out for testing
            # (?<!/) is a negative lookbehind to make sure the s3 key doesn't end with a /
            m = re.fullmatch(r"([A-Za-z0-9-]+)/(.+)(?<!/)", event["job_file_key"])

            # throws AttributeError if regex wasn't matched
            state_machine_name, remainder = m.groups()

            exec_name = make_execution_name(remainder, event["job_file_version"])
            logger.info(f"{exec_name=}")

            input_obj = {
                "job_file": {
                    "bucket": event["job_file_bucket"],
                    "key": event["job_file_key"],
                    "version": event["job_file_version"],
                },
                "index": event["branch"],
            }

            region = os.environ["REGION"]
            acct_num = os.environ["ACCT_NUM"]
            state_machine_arn = f"arn:aws:states:{region}:{acct_num}:stateMachine:{state_machine_name}:current"

            if "dry_run" not in event:
                response = sfn.start_execution(
                    stateMachineArn=state_machine_arn,
                    name=exec_name,
                    input=json.dumps(input_obj)
                )
                logger.info(f"{response=}")

        except AttributeError:
            logger.info("no workflow name found")

        except sfn.exceptions.ExecutionAlreadyExists:
            # duplicated s3 events are way more likely than bona fide name collisions
            logger.info(f"duplicate event: {exec_name}")

        # throws AccessDeniedException if state machine is not a bclaw workflow from this installation
        # throws StateMachineDoesNotExist if alias "current" does not exist on state machine