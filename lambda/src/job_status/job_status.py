from datetime import datetime, timedelta
import os

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError


def lambda_handler(event: dict, context: object) -> None:
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(os.environ["JOB_STATUS_TABLE"])

    for sns_record in event["Records"]:
        time_str = sns_record["Sns"]["Timestamp"]
        exec_id = sns_record["Sns"]["MessageAttributes"]["execution_id"]["Value"]
        wf_name = sns_record["Sns"]["MessageAttributes"]["workflow_name"]["Value"]

        # drop top level directory, it'll be the same as wf_name
        job_file_name = sns_record["Sns"]["MessageAttributes"]["job_file"]["Value"].split("/", 1)[1]
        job_file_version = sns_record["Sns"]["MessageAttributes"]["job_file_version"]["Value"]
        job_status = sns_record["Sns"]["MessageAttributes"]["status"]["Value"]

        timestamp = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%f%z")
        expiration = timestamp + timedelta(days=int(os.environ["EXPIRATION_DAYS"]))

        item = {
            "Item": {
                "workflowName": wf_name,
                "executionId": exec_id,
                "jobFileName": job_file_name,
                "jobFileVersion": job_file_version,
                "status": job_status,
                "timestamp": int(timestamp.timestamp()),
                "expiration": int(expiration.timestamp()),
            }
        }

        # notifications might arrive out of order: this prevents
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

