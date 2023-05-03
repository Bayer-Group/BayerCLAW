import json
import logging
import os
import re

import boto3

logger = logging.getLogger(__name__)


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
    norm_version = normalize(version) or "NULL"
    if replay == "":
        ret = f"{norm_key:.71}_{norm_version:.8}"
    else:
        norm_replay = normalize(replay)
        ret = f"{norm_replay:.10}_{norm_key:.60}_{norm_version:.8}"
    return ret


def make_state_machine_name(context: object) -> str:
    root = os.environ["SFN_NAME_ROOT"]
    if os.environ["VERSIONED_SFN"] == "true":
        version = context.function_version
        sfn_name = f"{root}--{version}"
    else:
        sfn_name = root
    logger.info(f"{sfn_name=}")
    return sfn_name


def main(event: dict, context: object) -> None:
    # event = {
    #   "branch": "..."
    #   "job_file_bucket": "...",
    #   "job_file_key": "...",
    #   "job_file_version": "..."  # empty string if launcher bucket versioning is suspended
    #   "replay": "...",  # empty string if not an archive replay
    # }

    sfn = boto3.client("stepfunctions")

    try:
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
            },
            "index": event["branch"],
        }

        region = os.environ["REGION"]
        acct_num = os.environ["ACCT_NUM"]
        state_machine_arn = f"arn:aws:states:{region}:{acct_num}:stateMachine:{make_state_machine_name(context)}"

        if "dry_run" not in event:
            response = sfn.start_execution(
                stateMachineArn=state_machine_arn,
                name=exec_name,
                input=json.dumps(input_obj)
            )
            logger.info(f"{response=}")

    except sfn.exceptions.ExecutionAlreadyExists:
        # duplicated s3 events are way more likely than bona fide name collisions
        logger.info(f"duplicate event: {exec_name}")
