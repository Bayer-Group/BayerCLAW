import datetime as dt
# from datetime import datetime, timedelta
import json
import os

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError


def lambda_handler(event: dict, context: object) -> None:
    print(f"{event=}")

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(os.environ["JOB_STATUS_TABLE"])

    # todo:
    #    should launcher put wf name in input object?
    #    should eventbridge rule put something bclaw-specific in input object for recognition?

    try:
        wf_name = event["detail"]["stateMachineArn"].rsplit(":", 1)[-1]
        exec_id = event["detail"]["name"]
        job_status = event["detail"]["status"]

        input_obj = json.loads(event["detail"]["input"])

        # HEY! this causes subpipe executions to look like superpipe executions
        # wf_name, job_file_name = input_obj["job_file"]["key"].split("/", 1)
        job_file_name = input_obj["job_file"]["key"].split("/", 1)[-1]
        job_file_version = input_obj["job_file"]["version"]

        time_str = event["time"]
        timestamp = dt.datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S%z")
        expiration = timestamp + dt.timedelta(days=int(os.environ["EXPIRATION_DAYS"]))

        item = {
            "Item": {
                "workflowName": wf_name,
                "executionId": exec_id,
                "jobFile": f"{job_file_name}#{job_file_version}",
                "status": job_status,
                "timestamp": int(timestamp.timestamp()),
                "expiration": int(expiration.timestamp()),
            }
        }

        # events might arrive out of order: this condition prevents
        # existing SUCCEEDED, FAILED, or ABORTED records in the table
        # from being overwritten by incoming RUNNING records
        if job_status == "RUNNING":
            item["ConditionExpression"] = (
                Attr("status").not_exists() |
                Attr("status").eq("RUNNING")
            )

        try:
            result = table.put_item(**item)
            print(str(result))

        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                pass

    except (KeyError, ValueError):
        print("not a bayerclaw execution")
