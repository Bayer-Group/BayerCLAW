import json
import os
import re

import boto3


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
    #   "Records": [
    #     {
    #       "body": <json string>,
    #       ...and other sqs stuff
    #     }
    #   ]
    # }

    # record = {
    #   "job_file": {
    #     "bucket": "...",
    #     "key": "...",
    #     "version": "..."
    #   },
    #   "replay": "...",  # empty string if not an archive replay
    #   "index": "main",
    #   "sfn_arn": "..."
    # }

    print(str(event))

    for record_str in event["Records"]:
        record = json.loads(record_str["body"])

        # todo: remove
        assert "_DIE_DIE_DIE_" not in record["job_file"]["key"]

        state_machine_arn = record.pop("sfn_arn")
        replay = record.pop("replay")
        exec_name = make_execution_name(record["job_file"]["key"],
                                        record["job_file"]["version"],
                                        replay)

        sfn = boto3.client("stepfunctions")

        try:
            while True:
                response = sfn.start_execution(
                    stateMachineArn=state_machine_arn,
                    name=exec_name,
                    input=json.dumps(record)
                )

                print(str(response))

                if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                    break

        except sfn.exceptions.ExecutionAlreadyExists:
            # duplicated s3 events are way more likely than bona fide name collisions
            print(f"duplicate event: {exec_name}")
