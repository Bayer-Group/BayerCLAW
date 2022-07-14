import json
import os
import re

import boto3


# def make_execution_name0(s3_key: str, version: str, max_len=80) -> str:
#     raw_name = os.path.splitext(os.path.basename(s3_key))[0]
#     replace illegal chars
    # normalized_name = re.sub(r"[^A-Za-z0-9]+", "_", raw_name)
    # shorten
    # shortened_name = normalized_name[:max_len - len(version) - 1]
    # append version
    # ret = shortened_name + "." + version
    # return ret


# def make_execution_name1(s3_key: str, version: str, max_len=80, version_len=8) -> str:
    # assume the filename extension is something uninformative like ".json"
    # key_root = os.path.splitext(s3_key)[0]
    # top level dir is constant for a workflow so discard it
    # key_root2 = key_root.split("/", 1)[1]

    # in case I ever want to enable step functions logging
    # normalized_version = version.replace(".", "-")
    # normalized_name = re.sub(r"[^A-Za-z0-9]+", "-", key_root2)

    # shortened_version = normalized_version[:version_len]
    # shortened_name = normalized_name[: max_len - 1 - len(shortened_version)].rstrip("-")
    # ret = shortened_name + "_" + shortened_version
    # return ret


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
