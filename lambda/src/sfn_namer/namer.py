import json
import os
import re

import boto3


def make_execution_name0(s3_key: str, version: str, max_len=80) -> str:
    raw_name = os.path.splitext(os.path.basename(s3_key))[0]
    # replace illegal chars
    normalized_name = re.sub(r"[^A-Za-z0-9]+", "_", raw_name)
    # shorten
    shortened_name = normalized_name[:max_len - len(version) - 1]
    # append version
    ret = shortened_name + "." + version
    return ret


def make_execution_name(s3_key: str, version: str, max_len=80, version_len=8) -> str:
    # assume the filename extension is something uninformative like ".json"
    key_root = os.path.splitext(s3_key)[0]
    # top level dir is constant for a workflow so discard it
    key_root2 = key_root.split("/", 1)[1]

    # in case I ever want to enable step functions logging
    normalized_version = version.replace(".", "-")
    normalized_name = re.sub(r"[^A-Za-z0-9]+", "-", key_root2)

    shortened_version = normalized_version[:version_len]
    shortened_name = normalized_name[: max_len - 1 - len(shortened_version)].rstrip("-")
    ret = shortened_name + "_" + shortened_version
    return ret


def lambda_handler(event: dict, context: object) -> None:
    # event = {
    #   "job_file": {
    #     "bucket": "...",
    #     "key": "...",
    #     "version": "..."
    #   },
    #   "index": "main",
    #   "sfn_arn": "..."
    # }
    print(str(event))

    state_machine_arn = event.pop("sfn_arn")
    exec_name = make_execution_name(event["job_file"]["key"], event["job_file"]["version"])

    sfn = boto3.client("stepfunctions")

    try:
        while True:
            response = sfn.start_execution(
                stateMachineArn=state_machine_arn,
                name=exec_name,
                input=json.dumps(event)
            )
            # todo: check response["ResponseMetadata"]["HTTPStatusCode"] == 200, crash if not

            print(str(response))

            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                break

    except sfn.exceptions.ExecutionAlreadyExists:
        # duplicated s3 events are way more likely than bona fide name collisions
        print(f"duplicate event: {exec_name}")
